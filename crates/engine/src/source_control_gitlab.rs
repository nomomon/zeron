//! The board's GitLab adapter, backed by the host's `glab` installation and
//! its sign-in. Merge requests are listed one project at a time through
//! GraphQL; their detail and comments open in the browser for now.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use zeron_proto::change_request_assessment::{ChangeRequestCi, CiState};
use zeron_proto::{
    ChangeRequestActor, ChangeRequestComment, ChangeRequestFilter, ChangeRequestListItem,
    ChangeRequestMergeability, ChangeRequestPage, ChangeRequestReviewDecision, ChangeRequestState,
};

use super::{
    ChangeRequestError, GITHUB_OUTPUT_LIMIT, GITHUB_TIMEOUT, PrRequestCache, PrResult,
    ProcessOutput, ProcessRequest, ProcessRunner, SystemProcessRunner, cached_request,
    classify_run_error, valid_page_cursor,
};
use crate::change_request_board::{
    ChangeRequestBoardProvider, ChangeRequestScope, GITHUB_HOST, GITLAB_HOST, GITLAB_PROVIDER,
    valid_host,
};
use crate::change_request_store::ChangeRequestStore;

/// `currentUser` rides along so each item can say whether it is the
/// viewer's. Names come from variables, never from the query text.
const GITLAB_LIST_QUERY: &str = "query($fullPath: ID!, $after: String, $author: String, $reviewer: String) { currentUser { username } project(fullPath: $fullPath) { fullPath mergeRequests(state: opened, sort: UPDATED_DESC, first: 50, after: $after, authorUsername: $author, reviewerUsername: $reviewer) { count pageInfo { hasNextPage endCursor } nodes { iid title draft webUrl createdAt updatedAt author { username } diffStatsSummary { additions deletions } diffHeadSha headPipeline { status sha } conflicts mergeStatusEnum approved approvalsLeft reviewers { nodes { username mergeRequestInteraction { reviewState } } } } } } }";
const GITLAB_PROJECT_QUERY: &str =
    "query($fullPath: ID!) { project(fullPath: $fullPath) { fullPath } }";
const GITLAB_VIEWER_QUERY: &str = "query { currentUser { username } }";
/// Tokens `glab` prefers over its stored sign-in, which may belong to
/// another account than the config's `user`.
const GITLAB_TOKEN_VARIABLES: [&str; 3] = ["GITLAB_TOKEN", "GITLAB_ACCESS_TOKEN", "OAUTH_TOKEN"];

/// GitLab provider backed by `glab`. Hosts are gitlab.com and any host
/// `glab` is signed in to; every call names its host with `--hostname`.
#[derive(Clone)]
pub(crate) struct GitLabCli {
    runner: Arc<dyn ProcessRunner>,
    pr_cache: Arc<tokio::sync::Mutex<PrRequestCache>>,
    /// The board's on-disk first pages, as for GitHub.
    store: Option<ChangeRequestStore>,
    /// Self-managed hosts from `glab`'s config, read once per engine.
    hosts: Arc<OnceLock<Vec<String>>>,
    /// The signed-in username per host and when it was read.
    users: Arc<tokio::sync::Mutex<HashMap<String, UserRead>>>,
}

/// A username, if any, and when it was read.
type UserRead = (Instant, Option<String>);

impl GitLabCli {
    pub(crate) fn new() -> Self {
        let gitlab = Self::with_runner(Arc::new(SystemProcessRunner));
        // Tests never read the user's own glab configuration.
        if cfg!(test) {
            gitlab
        } else {
            Self {
                hosts: Default::default(),
                ..gitlab
            }
        }
    }

    pub(super) fn with_runner(runner: Arc<dyn ProcessRunner>) -> Self {
        Self {
            runner,
            pr_cache: Default::default(),
            store: None,
            hosts: Arc::new(OnceLock::from(Vec::new())),
            users: Default::default(),
        }
    }

    /// Keep the board's first pages in `data_dir`, so it opens at once.
    pub(crate) fn with_store(mut self, data_dir: &std::path::Path) -> Self {
        self.store = Some(ChangeRequestStore::new(data_dir));
        self
    }

    /// Serve these self-managed hosts, as if `glab` were signed in to them.
    #[cfg(test)]
    pub(crate) fn with_hosts(mut self, hosts: &[&str]) -> Self {
        self.hosts = Arc::new(OnceLock::from(
            hosts
                .iter()
                .map(|host| host.to_string())
                .collect::<Vec<_>>(),
        ));
        self
    }

    async fn cached_pr_request<F>(&self, key: String, refresh: bool, fetch: F) -> PrResult
    where
        F: std::future::Future<Output = PrResult> + Send + 'static,
    {
        cached_request(&self.pr_cache, key, refresh, fetch).await
    }

    fn configured_hosts(&self) -> &[String] {
        self.hosts.get_or_init(|| {
            glab_config_files()
                .into_iter()
                .filter_map(|path| std::fs::read_to_string(path).ok())
                .flat_map(|config| config_hosts(&config))
                .collect()
        })
    }

    /// A scope this adapter may pass to `glab`, checked again whatever the
    /// caller did.
    fn served_scope(&self, scope: &ChangeRequestScope) -> Option<ChangeRequestScope> {
        (scope.provider == GITLAB_PROVIDER && self.serves_host(&scope.host))
            .then(|| ChangeRequestScope::new(GITLAB_PROVIDER, &scope.host, &scope.path))
            .flatten()
    }

    /// The account in `glab`'s config for `host`, read locally and re-read
    /// every 30 seconds so a new `glab auth login` takes effect.
    async fn configured_user(&self, host: &str) -> Option<String> {
        let mut users = self.users.lock().await;
        if let Some((read_at, user)) = users.get(host)
            && read_at.elapsed() < Duration::from_secs(30)
        {
            return user.clone();
        }
        let user = self
            .runner
            .run(ProcessRequest {
                args: vec![
                    "config".into(),
                    "get".into(),
                    "user".into(),
                    "--host".into(),
                    host.into(),
                ],
                timeout: Duration::from_secs(5),
                ..gitlab_request()
            })
            .await
            .ok()
            .filter(|output| output.success && !output.stdout_truncated)
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|user| user.trim().to_owned())
            .filter(|user| valid_username(user));
        users.insert(host.to_owned(), (Instant::now(), user.clone()));
        user
    }

    /// Who the board's filters mean by "me" on `host`: the configured
    /// account, else whoever the token in use belongs to.
    async fn viewer(&self, host: &str) -> Option<String> {
        if !token_in_environment()
            && let Some(user) = self.configured_user(host).await
        {
            return Some(user);
        }
        let gitlab = self.clone();
        let target = host.to_owned();
        let value = self
            .cached_pr_request(format!("viewer:{host}"), false, async move {
                let output = gitlab
                    .graphql(&target, GITLAB_VIEWER_QUERY, Vec::new())
                    .await?;
                Ok(output["data"]["currentUser"]["username"].clone())
            })
            .await
            .ok()?;
        value
            .as_str()
            .filter(|user| valid_username(user))
            .map(str::to_owned)
    }

    /// Run one GraphQL query on `host` with string `variables`, returning
    /// the decoded response.
    async fn graphql(
        &self,
        host: &str,
        query: &str,
        variables: Vec<(&str, String)>,
    ) -> Result<serde_json::Value, ChangeRequestError> {
        let output = self.run_graphql(host, query, variables).await?;
        if !output.success {
            return Err(classify_gitlab_failure(&output.stderr));
        }
        if output.stdout_truncated {
            return Err(ChangeRequestError::Decode);
        }
        serde_json::from_slice(&output.stdout).map_err(|_| ChangeRequestError::Decode)
    }

    async fn run_graphql(
        &self,
        host: &str,
        query: &str,
        variables: Vec<(&str, String)>,
    ) -> Result<ProcessOutput, ChangeRequestError> {
        if !valid_host(host) {
            return Err(ChangeRequestError::UnsupportedRepository);
        }
        let mut args = vec![
            "api".into(),
            "graphql".into(),
            "--hostname".into(),
            host.into(),
            "-f".into(),
            format!("query={query}"),
        ];
        for (name, value) in variables {
            args.extend(["-f".into(), format!("{name}={value}")]);
        }
        self.runner
            .run(ProcessRequest {
                args,
                ..gitlab_request()
            })
            .await
            .map_err(classify_run_error)
    }

    async fn fetch_list_page(
        &self,
        scope: &ChangeRequestScope,
        filter: ChangeRequestFilter,
        after: Option<&str>,
        refresh: bool,
    ) -> Result<ChangeRequestPage, ChangeRequestError> {
        let key = format!(
            "list:{}:{filter:?}:{}",
            scope.to_wire().to_ascii_lowercase(),
            after.unwrap_or_default()
        );
        let gitlab = self.clone();
        let scope = scope.clone();
        let after = after.map(str::to_owned);
        let result = self
            .cached_pr_request(key, refresh, async move {
                let page = gitlab.fetch_page(&scope, filter, after.as_deref()).await?;
                serde_json::to_value(page).map_err(|_| ChangeRequestError::Decode)
            })
            .await?;
        serde_json::from_value(result).map_err(|_| ChangeRequestError::Decode)
    }

    async fn fetch_page(
        &self,
        scope: &ChangeRequestScope,
        filter: ChangeRequestFilter,
        after: Option<&str>,
    ) -> Result<ChangeRequestPage, ChangeRequestError> {
        let mut variables = vec![("fullPath", scope.path.clone())];
        let person = match filter {
            ChangeRequestFilter::All => None,
            ChangeRequestFilter::Authored => Some("author"),
            ChangeRequestFilter::Reviewing => Some("reviewer"),
        };
        if let Some(person) = person {
            // Without a known account, "mine" can't be asked for.
            let viewer = self
                .viewer(&scope.host)
                .await
                .ok_or(ChangeRequestError::Authentication)?;
            variables.push((person, viewer));
        }
        if let Some(after) = after {
            variables.push(("after", after.to_owned()));
        }
        let output = self
            .run_graphql(&scope.host, GITLAB_LIST_QUERY, variables)
            .await?;
        let response = (!output.stdout_truncated)
            .then(|| serde_json::from_slice::<GlListResponse>(&output.stdout).ok())
            .flatten();
        // Like `gh`, `glab` exits non-zero whenever the response carries
        // `errors`. A page whose project resolved is still usable.
        let response = match response {
            Some(response)
                if output.success
                    || response
                        .data
                        .as_ref()
                        .is_some_and(|data| data.project.is_some()) =>
            {
                response
            }
            _ if !output.success => return Err(classify_gitlab_failure(&output.stderr)),
            _ => return Err(ChangeRequestError::Decode),
        };
        let partial_metadata = !response.errors.is_empty();
        let data = response.data.ok_or(ChangeRequestError::Decode)?;
        let viewer = data
            .current_user
            .map(|user| user.username)
            .filter(|user| valid_username(user));
        // GitLab answers a project that doesn't exist, or that the viewer
        // can't see, with null. Signed out, every private project looks so.
        let Some(project) = data.project else {
            return Err(match viewer {
                None => ChangeRequestError::Authentication,
                Some(_) => ChangeRequestError::UnsupportedRepository,
            });
        };
        // GitLab's name for the project, which the board adopts.
        let canonical = ChangeRequestScope::new(GITLAB_PROVIDER, &scope.host, &project.full_path)
            .ok_or(ChangeRequestError::Decode)?;
        let requests = project.merge_requests.ok_or(ChangeRequestError::Decode)?;
        let mut items = requests
            .nodes
            .into_iter()
            .flatten()
            .map(|request| to_list_item(request, &canonical, viewer.as_deref()))
            .collect::<Result<Vec<_>, _>>()?;
        for item in &mut items {
            if partial_metadata {
                item.ci = Default::default();
                item.viewer_did_author = None;
                item.viewer_review_requested = None;
            }
            if filter == ChangeRequestFilter::Authored {
                item.viewer_did_author = Some(true);
            }
        }
        items.truncate(50);
        items.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.number.cmp(&right.number))
        });
        let next_cursor = requests
            .page_info
            .filter(|info| info.has_next_page && !items.is_empty())
            .and_then(|info| info.end_cursor)
            .filter(|cursor| valid_page_cursor(cursor));
        Ok(ChangeRequestPage {
            items,
            next_cursor,
            total_count: requests.count,
            fetched_at: Some(Utc::now()),
        })
    }
}

/// Board requests name their host and never prompt.
fn gitlab_request() -> ProcessRequest {
    ProcessRequest {
        program: "glab".into(),
        args: Vec::new(),
        stdin: None,
        cwd: None,
        env: vec![("GLAB_NO_PROMPT".into(), "true".into())],
        timeout: GITHUB_TIMEOUT,
        output_limit: GITHUB_OUTPUT_LIMIT,
    }
}

fn token_in_environment() -> bool {
    GITLAB_TOKEN_VARIABLES
        .iter()
        .any(|name| std::env::var_os(name).is_some_and(|value| !value.is_empty()))
}

fn valid_username(user: &str) -> bool {
    (1..=255).contains(&user.len())
        && user
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

/// Where `glab` may keep its config: `GLAB_CONFIG_DIR`, the platform's
/// config directory, and the older `~/.config`.
fn glab_config_files() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Some(dir) = std::env::var_os("GLAB_CONFIG_DIR").filter(|dir| !dir.is_empty()) {
        dirs.push(PathBuf::from(dir));
    }
    if let Some(dir) = std::env::var_os("XDG_CONFIG_HOME").filter(|dir| !dir.is_empty()) {
        dirs.push(PathBuf::from(dir).join("glab-cli"));
    }
    let home = crate::repos::home_dir();
    #[cfg(target_os = "macos")]
    dirs.push(home.join("Library/Application Support/glab-cli"));
    #[cfg(windows)]
    if let Some(dir) = std::env::var_os("APPDATA").filter(|dir| !dir.is_empty()) {
        dirs.push(PathBuf::from(dir).join("glab-cli"));
    }
    dirs.push(home.join(".config/glab-cli"));
    dirs.into_iter().map(|dir| dir.join("config.yml")).collect()
}

/// The keys of the top-level `hosts:` map in `glab`'s YAML config, read by
/// indentation alone; anything unexpected is skipped.
fn config_hosts(config: &str) -> Vec<String> {
    let mut hosts = Vec::new();
    let mut in_hosts = false;
    let mut indent = None;
    for line in config.lines() {
        let content = line.trim_start();
        if content.is_empty() || content.starts_with('#') {
            continue;
        }
        let depth = line.len() - content.len();
        if depth == 0 {
            in_hosts = content.trim_end() == "hosts:";
            indent = None;
            continue;
        }
        if in_hosts
            && depth == *indent.get_or_insert(depth)
            && let Some(host) = content.trim_end().strip_suffix(':')
        {
            let host = host.trim_matches(['"', '\'']).to_ascii_lowercase();
            if valid_host(&host) && host != GITHUB_HOST && !hosts.contains(&host) {
                hosts.push(host);
            }
        }
    }
    hosts
}

/// `glab` prints its own sign-in hints; never expose them, only classify.
fn classify_gitlab_failure(stderr: &[u8]) -> ChangeRequestError {
    let stderr = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    if stderr.contains("rate limit") || stderr.contains("too many requests") {
        ChangeRequestError::RateLimited
    } else if stderr.contains("unauthenticated")
        || stderr.contains("unauthorized")
        || stderr.contains("invalid token")
        || stderr.contains("token is expired")
        || stderr.contains("not logged")
        || stderr.contains("glab auth login")
        || stderr.contains("authentication")
    {
        ChangeRequestError::Authentication
    } else {
        ChangeRequestError::CommandFailed
    }
}

/// How a merge request fills the board's provider-neutral row, so the same
/// assessment groups it as it would a pull request:
///
/// - `iid` is the number, shown as `!iid`; `webUrl` the URL; `draft` makes
///   it a draft; the project's full path, qualified by host, its repository.
/// - CI is the head pipeline, and only when it ran on `diffHeadSha`: a
///   pipeline for an older commit says nothing about the current one, as
///   with GitHub's rollup. `SUCCESS` passed; `FAILED` and `CANCELED` failed;
///   created, waiting, preparing, pending, running, scheduled and manual
///   pipelines are pending; `SKIPPED` skipped; no pipeline is no checks.
/// - Conflicts, or `CANNOT_BE_MERGED`, conflict; `CAN_BE_MERGED` is
///   mergeable; anything else, such as a check GitLab hasn't run yet, is
///   unknown.
/// - Review: any reviewer requesting changes, else approved, else review
///   required while approvals are left or a reviewer hasn't reviewed, else
///   no decision.
/// - The viewer authored it when they are its author, and their review is
///   requested while they are a reviewer who hasn't reviewed yet.
fn to_list_item(
    request: GlMergeRequest,
    scope: &ChangeRequestScope,
    viewer: Option<&str>,
) -> Result<ChangeRequestListItem, ChangeRequestError> {
    let number = request
        .iid
        .parse::<u64>()
        .ok()
        .filter(|number| *number > 0)
        .ok_or(ChangeRequestError::Decode)?;
    let title = request
        .title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if title.is_empty() {
        return Err(ChangeRequestError::Decode);
    }
    let url =
        reqwest::Url::parse(request.web_url.trim()).map_err(|_| ChangeRequestError::Decode)?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(ChangeRequestError::Decode);
    }
    let head = request.diff_head_sha.unwrap_or_default();
    let ci = match &request.head_pipeline {
        None => ChangeRequestCi {
            state: CiState::NoChecks,
            total_count: 0,
        },
        Some(pipeline) => ChangeRequestCi {
            state: if pipeline.sha.as_deref() == Some(head.as_str()) {
                pipeline_state(&pipeline.status)
            } else {
                CiState::Unknown
            },
            total_count: 1,
        },
    };
    let reviews: Vec<(&str, &str)> = request
        .reviewers
        .iter()
        .flat_map(|reviewers| reviewers.nodes.iter().flatten())
        .map(|reviewer| {
            (
                reviewer.username.as_str(),
                reviewer
                    .merge_request_interaction
                    .as_ref()
                    .and_then(|interaction| interaction.review_state.as_deref())
                    .unwrap_or_default(),
            )
        })
        .collect();
    let review_decision = if reviews
        .iter()
        .any(|(_, state)| *state == "REQUESTED_CHANGES")
    {
        ChangeRequestReviewDecision::ChangesRequested
    } else if request.approved == Some(true) {
        ChangeRequestReviewDecision::Approved
    } else if request.approvals_left.is_some_and(|left| left > 0)
        || reviews.iter().any(|(_, state)| awaiting_review(state))
    {
        ChangeRequestReviewDecision::ReviewRequired
    } else {
        ChangeRequestReviewDecision::Unknown
    };
    let mergeability = match request.merge_status_enum.as_deref() {
        _ if request.conflicts == Some(true) => ChangeRequestMergeability::Conflicting,
        Some("CANNOT_BE_MERGED") => ChangeRequestMergeability::Conflicting,
        Some("CAN_BE_MERGED") => ChangeRequestMergeability::Mergeable,
        _ => ChangeRequestMergeability::Unknown,
    };
    let author = request
        .author
        .map(|author| author.username)
        .unwrap_or_default();
    let (stats_additions, stats_deletions) = request
        .diff_stats_summary
        .map_or((0, 0), |stats| (stats.additions, stats.deletions));
    Ok(ChangeRequestListItem {
        provider: GITLAB_PROVIDER.into(),
        viewer_did_author: viewer.map(|viewer| author.eq_ignore_ascii_case(viewer)),
        viewer_review_requested: viewer.map(|viewer| {
            reviews
                .iter()
                .any(|(user, state)| user.eq_ignore_ascii_case(viewer) && awaiting_review(state))
        }),
        author: ChangeRequestActor { login: author },
        head_ref_oid: head,
        ci,
        repository: scope.to_wire(),
        number,
        title,
        url: url.to_string(),
        state: ChangeRequestState::Open,
        is_draft: request.draft,
        review_decision,
        additions: stats_additions,
        deletions: stats_deletions,
        mergeability,
        created_at: request.created_at,
        updated_at: request.updated_at,
    })
}

fn pipeline_state(status: &str) -> CiState {
    match status {
        "SUCCESS" => CiState::Passed,
        "FAILED" | "CANCELED" | "CANCELING" => CiState::Failed,
        "CREATED"
        | "WAITING_FOR_RESOURCE"
        | "WAITING_FOR_CALLBACK"
        | "PREPARING"
        | "PENDING"
        | "RUNNING"
        | "SCHEDULED"
        | "MANUAL" => CiState::Pending,
        "SKIPPED" => CiState::Skipped,
        _ => CiState::Unknown,
    }
}

/// A reviewer who hasn't reviewed yet, or whose approval was withdrawn.
fn awaiting_review(state: &str) -> bool {
    matches!(state, "UNREVIEWED" | "REVIEW_STARTED" | "UNAPPROVED")
}

/// `https://<host>/<group>/.../<project>/-/merge_requests/<iid>`, with the
/// project's scope and iid.
fn merge_request_url(raw: &str) -> Option<(ChangeRequestScope, u64)> {
    let url = reqwest::Url::parse(raw).ok()?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
    {
        return None;
    }
    let path = url.path().trim_end_matches('/');
    let (project, iid) = path
        .trim_start_matches('/')
        .split_once("/-/merge_requests/")?;
    let iid = iid.parse::<u64>().ok().filter(|iid| *iid > 0)?;
    let scope = ChangeRequestScope::new(GITLAB_PROVIDER, url.host_str()?, project)?;
    Some((scope, iid))
}

#[async_trait]
impl ChangeRequestBoardProvider for GitLabCli {
    fn provider_id(&self) -> &'static str {
        GITLAB_PROVIDER
    }

    fn serves_host(&self, host: &str) -> bool {
        let host = host.to_ascii_lowercase();
        host != GITHUB_HOST && (host == GITLAB_HOST || self.configured_hosts().contains(&host))
    }

    fn owns_url(&self, url: &str) -> bool {
        merge_request_url(url).is_some_and(|(scope, _)| self.serves_host(&scope.host))
    }

    /// `gitlab:<host>:<username>`, from `glab`'s config. A token in the
    /// environment may belong to anyone, so then nothing is stored.
    async fn account_key(&self, host: &str) -> Option<String> {
        if token_in_environment() || !self.serves_host(host) {
            return None;
        }
        let host = host.to_ascii_lowercase();
        let user = self.configured_user(&host).await?;
        Some(format!("{GITLAB_PROVIDER}:{host}:{user}"))
    }

    /// GitLab's own path for the project, through `project(fullPath:)`.
    async fn canonical_scope(&self, scope: &ChangeRequestScope) -> Option<ChangeRequestScope> {
        let scope = self.served_scope(scope)?;
        let key = format!("project:{}", scope.to_wire().to_ascii_lowercase());
        let gitlab = self.clone();
        let (host, path) = (scope.host.clone(), scope.path.clone());
        let lookup = self.cached_pr_request(key, false, async move {
            let value = gitlab
                .graphql(&host, GITLAB_PROJECT_QUERY, vec![("fullPath", path)])
                .await?;
            Ok(value["data"]["project"]["fullPath"].clone())
        });
        let value = tokio::time::timeout(Duration::from_secs(5), lookup)
            .await
            .ok()?
            .ok()?;
        ChangeRequestScope::new(GITLAB_PROVIDER, &scope.host, value.as_str()?)
    }

    async fn list_page(
        &self,
        scope: &ChangeRequestScope,
        filter: ChangeRequestFilter,
        after: Option<&str>,
        refresh: bool,
        cached: bool,
    ) -> Result<ChangeRequestPage, ChangeRequestError> {
        let scope = self
            .served_scope(scope)
            .ok_or(ChangeRequestError::UnsupportedRepository)?;
        if after.is_some_and(|cursor| !valid_page_cursor(cursor)) {
            return Err(ChangeRequestError::Decode);
        }
        crate::change_request_board::stored_page(
            self,
            self.store.as_ref(),
            &scope,
            filter,
            after,
            refresh,
            cached,
            self.fetch_list_page(&scope, filter, after, refresh),
        )
        .await
    }

    /// Not yet: GitLab merge requests open in the browser.
    async fn change_request(
        &self,
        _url: &str,
        _diff: bool,
        _refresh: bool,
        _head: Option<&str>,
        _base: Option<&str>,
    ) -> Result<serde_json::Value, ChangeRequestError> {
        Err(ChangeRequestError::UnsupportedRepository)
    }

    /// Not yet: comment on GitLab merge requests in the browser.
    async fn post_comment(
        &self,
        _url: &str,
        _body: &str,
    ) -> Result<ChangeRequestComment, ChangeRequestError> {
        Err(ChangeRequestError::UnsupportedRepository)
    }
}

#[derive(Debug, Deserialize)]
struct GlListResponse {
    #[serde(default)]
    data: Option<GlListData>,
    #[serde(default)]
    errors: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GlListData {
    #[serde(default)]
    current_user: Option<GlUser>,
    #[serde(default)]
    project: Option<GlProject>,
}

#[derive(Debug, Deserialize)]
struct GlUser {
    username: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GlProject {
    full_path: String,
    #[serde(default)]
    merge_requests: Option<GlMergeRequests>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GlMergeRequests {
    #[serde(default)]
    count: Option<u64>,
    #[serde(default)]
    page_info: Option<GlPageInfo>,
    nodes: Vec<Option<GlMergeRequest>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GlPageInfo {
    has_next_page: bool,
    end_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GlMergeRequest {
    iid: String,
    title: String,
    #[serde(default)]
    draft: bool,
    web_url: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    #[serde(default)]
    author: Option<GlUser>,
    #[serde(default)]
    diff_stats_summary: Option<GlDiffStats>,
    #[serde(default)]
    diff_head_sha: Option<String>,
    #[serde(default)]
    head_pipeline: Option<GlPipeline>,
    #[serde(default)]
    conflicts: Option<bool>,
    #[serde(default)]
    merge_status_enum: Option<String>,
    #[serde(default)]
    approved: Option<bool>,
    #[serde(default)]
    approvals_left: Option<i64>,
    #[serde(default)]
    reviewers: Option<GlReviewers>,
}

#[derive(Debug, Deserialize)]
struct GlDiffStats {
    additions: u64,
    deletions: u64,
}

#[derive(Debug, Deserialize)]
struct GlPipeline {
    status: String,
    #[serde(default)]
    sha: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GlReviewers {
    nodes: Vec<Option<GlReviewer>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GlReviewer {
    username: String,
    #[serde(default)]
    merge_request_interaction: Option<GlInteraction>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GlInteraction {
    #[serde(default)]
    review_state: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use zeron_proto::change_request_assessment::{AttentionReason, Blocker, Facts};

    use super::super::tests::{FakeProcessRunner, command_failure, command_success};
    use super::*;
    use crate::source_control::ProcessRunError;

    const PROJECT: &str = "gitlab.com/acme/platform/team/api";

    fn scope() -> ChangeRequestScope {
        ChangeRequestScope::from_wire(PROJECT).unwrap()
    }

    fn head(c: char) -> String {
        c.to_string().repeat(40)
    }

    fn merge_request(iid: u64, author: &str) -> serde_json::Value {
        json!({
            "iid": iid.to_string(),
            "title": format!("  Change\n{iid} "),
            "draft": false,
            "webUrl": format!("https://gitlab.com/acme/platform/team/api/-/merge_requests/{iid}"),
            "createdAt": "2026-08-10T09:30:00Z",
            "updatedAt": format!("2026-08-19T{:02}:00:00Z", iid % 24),
            "author": {"username": author},
            "diffStatsSummary": {"additions": iid, "deletions": 1},
            "diffHeadSha": head('a'),
            "headPipeline": {"status": "SUCCESS", "sha": head('a')},
            "conflicts": false,
            "mergeStatusEnum": "CAN_BE_MERGED",
            "approved": false,
            "approvalsLeft": 0,
            "reviewers": {"nodes": []},
        })
    }

    fn reviewer(username: &str, state: &str) -> serde_json::Value {
        json!({"username": username, "mergeRequestInteraction": {"reviewState": state}})
    }

    fn list_response(
        nodes: Vec<serde_json::Value>,
        cursor: Option<&str>,
        viewer: Option<&str>,
    ) -> Result<super::super::ProcessOutput, ProcessRunError> {
        let count = nodes.len();
        command_success(
            serde_json::to_vec(&json!({"data": {
                "currentUser": viewer.map(|username| json!({"username": username})),
                "project": {
                    "fullPath": "acme/platform/team/api",
                    "mergeRequests": {
                        "count": count + 10,
                        "pageInfo": {"hasNextPage": cursor.is_some(), "endCursor": cursor},
                        "nodes": nodes,
                    }
                }
            }}))
            .unwrap(),
        )
    }

    fn graphql_args(variables: &[(&str, &str)]) -> Vec<String> {
        let mut args: Vec<String> = ["api", "graphql", "--hostname", "gitlab.com", "-f"]
            .map(String::from)
            .into();
        args.push(format!("query={GITLAB_LIST_QUERY}"));
        for (name, value) in variables {
            args.extend(["-f".into(), format!("{name}={value}")]);
        }
        args
    }

    #[tokio::test]
    async fn merge_requests_map_onto_board_rows() {
        let mut draft = merge_request(1, "devuser");
        draft["draft"] = true.into();
        let mut failing = merge_request(2, "colleague");
        failing["headPipeline"]["status"] = "FAILED".into();
        failing["conflicts"] = true.into();
        failing["reviewers"]["nodes"] = json!([reviewer("DeVuSeR", "UNREVIEWED")]);
        let mut stale = merge_request(3, "colleague");
        stale["headPipeline"]["sha"] = head('b').into();
        stale["approved"] = true.into();
        stale["reviewers"]["nodes"] = json!([reviewer("devuser", "APPROVED")]);
        let mut feedback = merge_request(4, "devuser");
        feedback["headPipeline"]["status"] = "RUNNING".into();
        feedback["mergeStatusEnum"] = "UNCHECKED".into();
        feedback["approved"] = true.into();
        feedback["reviewers"]["nodes"] = json!([
            reviewer("lead", "APPROVED"),
            reviewer("other", "REQUESTED_CHANGES")
        ]);
        let mut waiting = merge_request(5, "colleague");
        waiting["headPipeline"] = serde_json::Value::Null;
        waiting["mergeStatusEnum"] = "CANNOT_BE_MERGED".into();
        waiting["approvalsLeft"] = 1.into();
        waiting["diffStatsSummary"] = serde_json::Value::Null;
        let mut canceled = merge_request(6, "colleague");
        canceled["headPipeline"]["status"] = "CANCELED".into();
        canceled["reviewers"]["nodes"] = json!([reviewer("devuser", "REVIEWED")]);
        let runner = FakeProcessRunner::with_responses([list_response(
            vec![draft, failing, stale, feedback, waiting, canceled],
            None,
            Some("devuser"),
        )]);
        let gitlab = GitLabCli::with_runner(runner.clone());
        // The board may ask with any casing; GitLab's own path comes back.
        let asked =
            ChangeRequestScope::from_wire("gitlab.com/Acme/Platform/team/api")
                .unwrap();
        let page = gitlab
            .list_page(&asked, ChangeRequestFilter::All, None, false, false)
            .await
            .unwrap();
        let request = &runner.requests()[0];
        assert_eq!(request.program, "glab");
        assert_eq!(
            request.args,
            graphql_args(&[("fullPath", "Acme/Platform/team/api")])
        );
        assert_eq!(
            request.env,
            [("GLAB_NO_PROMPT".to_string(), "true".to_string())]
        );
        assert_eq!(page.total_count, Some(16));
        assert_eq!(page.next_cursor, None);
        let items: HashMap<u64, &ChangeRequestListItem> =
            page.items.iter().map(|item| (item.number, item)).collect();
        assert_eq!(page.items[0].number, 6, "newest updates first");
        for item in &page.items {
            assert_eq!(item.provider, "gitlab");
            assert_eq!(item.repository, PROJECT);
            assert_eq!(item.state, ChangeRequestState::Open);
            assert_eq!(item.title, format!("Change {}", item.number));
            assert_eq!(item.head_ref_oid, head('a'));
        }
        let first = items[&1];
        assert!(first.is_draft);
        assert_eq!(first.author.login, "devuser");
        assert_eq!(
            first.url,
            "https://gitlab.com/acme/platform/team/api/-/merge_requests/1"
        );
        assert_eq!((first.additions, first.deletions), (1, 1));
        assert_eq!(first.ci.state, CiState::Passed);
        assert_eq!(first.mergeability, ChangeRequestMergeability::Mergeable);
        assert_eq!(first.review_decision, ChangeRequestReviewDecision::Unknown);
        assert_eq!(first.viewer_did_author, Some(true));
        assert_eq!(first.viewer_review_requested, Some(false));

        let failing = items[&2];
        assert_eq!(failing.ci.state, CiState::Failed);
        assert_eq!(failing.mergeability, ChangeRequestMergeability::Conflicting);
        assert_eq!(
            failing.review_decision,
            ChangeRequestReviewDecision::ReviewRequired
        );
        assert_eq!(failing.viewer_did_author, Some(false));
        assert_eq!(failing.viewer_review_requested, Some(true));
        let assessment = Facts::from_item(failing).assess();
        assert_eq!(
            assessment.blockers,
            [Blocker::FailingCi, Blocker::MergeConflicts]
        );
        assert_eq!(
            assessment.attention,
            [AttentionReason::ReviewRequested, AttentionReason::FailingCi],
            "needs you"
        );

        let stale = items[&3];
        assert_eq!(
            stale.ci.state,
            CiState::Unknown,
            "pipeline for an older head"
        );
        assert_eq!(stale.review_decision, ChangeRequestReviewDecision::Approved);
        assert_eq!(
            stale.viewer_review_requested,
            Some(false),
            "already approved"
        );

        let feedback = items[&4];
        assert_eq!(feedback.ci.state, CiState::Pending);
        assert_eq!(feedback.mergeability, ChangeRequestMergeability::Unknown);
        assert_eq!(
            feedback.review_decision,
            ChangeRequestReviewDecision::ChangesRequested,
            "changes requested outweighs an approval"
        );
        assert_eq!(
            Facts::from_item(feedback).assess().attention,
            [AttentionReason::FeedbackOnAuthored]
        );

        let waiting = items[&5];
        assert_eq!(waiting.ci.state, CiState::NoChecks);
        assert_eq!(waiting.mergeability, ChangeRequestMergeability::Conflicting);
        assert_eq!(
            waiting.review_decision,
            ChangeRequestReviewDecision::ReviewRequired
        );
        assert_eq!((waiting.additions, waiting.deletions), (0, 0));

        let canceled = items[&6];
        assert_eq!(canceled.ci.state, CiState::Failed);
        assert_eq!(
            canceled.review_decision,
            ChangeRequestReviewDecision::Unknown
        );
        assert_eq!(canceled.viewer_review_requested, Some(false));
    }

    #[test]
    fn pipeline_states_map_onto_ci() {
        for (status, state) in [
            ("SUCCESS", CiState::Passed),
            ("FAILED", CiState::Failed),
            ("CANCELED", CiState::Failed),
            ("CANCELING", CiState::Failed),
            ("CREATED", CiState::Pending),
            ("WAITING_FOR_RESOURCE", CiState::Pending),
            ("WAITING_FOR_CALLBACK", CiState::Pending),
            ("PREPARING", CiState::Pending),
            ("PENDING", CiState::Pending),
            ("RUNNING", CiState::Pending),
            ("SCHEDULED", CiState::Pending),
            ("MANUAL", CiState::Pending),
            ("SKIPPED", CiState::Skipped),
            ("SOMETHING_NEW", CiState::Unknown),
        ] {
            assert_eq!(pipeline_state(status), state, "{status}");
        }
    }

    #[tokio::test]
    async fn filters_ask_for_the_viewer_and_pages_follow_the_cursor() {
        let cursor = "eyJpZCI6IjQ0MSJ9";
        let runner = FakeProcessRunner::with_responses([
            command_success("devuser\n"),
            list_response(
                vec![merge_request(9, "devuser")],
                Some(cursor),
                Some("devuser"),
            ),
            list_response(vec![merge_request(8, "devuser")], None, Some("devuser")),
            list_response(vec![merge_request(7, "colleague")], None, Some("devuser")),
            list_response(vec![merge_request(6, "colleague")], None, Some("devuser")),
        ]);
        let gitlab = GitLabCli::with_runner(runner.clone());
        let path = "acme/platform/team/api";
        let first = gitlab
            .list_page(&scope(), ChangeRequestFilter::Authored, None, false, false)
            .await
            .unwrap();
        assert_eq!(first.next_cursor.as_deref(), Some(cursor));
        assert_eq!(first.items[0].viewer_did_author, Some(true));
        let second = gitlab
            .list_page(
                &scope(),
                ChangeRequestFilter::Authored,
                Some(cursor),
                false,
                false,
            )
            .await
            .unwrap();
        assert_eq!(second.items[0].number, 8);
        let reviewing = gitlab
            .list_page(&scope(), ChangeRequestFilter::Reviewing, None, false, false)
            .await
            .unwrap();
        // Being a reviewer lists it; only an unreviewed request needs you.
        assert_eq!(reviewing.items[0].viewer_review_requested, Some(false));
        gitlab
            .list_page(&scope(), ChangeRequestFilter::All, None, false, false)
            .await
            .unwrap();
        let requests = runner.requests();
        assert_eq!(
            requests[0].args,
            ["config", "get", "user", "--host", "gitlab.com"]
        );
        assert_eq!(
            requests[1].args,
            graphql_args(&[("fullPath", path), ("author", "devuser")])
        );
        assert_eq!(
            requests[2].args,
            graphql_args(&[("fullPath", path), ("author", "devuser"), ("after", cursor)])
        );
        // The account is read once, not per page.
        assert_eq!(
            requests[3].args,
            graphql_args(&[("fullPath", path), ("reviewer", "devuser")])
        );
        assert_eq!(requests[4].args, graphql_args(&[("fullPath", path)]));
        assert_eq!(requests.len(), 5);
        // Cursors are checked before they reach glab.
        assert_eq!(
            gitlab
                .list_page(
                    &scope(),
                    ChangeRequestFilter::All,
                    Some("$(nope)"),
                    false,
                    false
                )
                .await
                .err(),
            Some(ChangeRequestError::Decode)
        );
        assert_eq!(runner.requests().len(), 5);
    }

    #[tokio::test]
    async fn without_a_configured_account_filters_ask_gitlab_who_is_signed_in() {
        let runner = FakeProcessRunner::with_responses([
            command_success(""),
            command_success(r#"{"data":{"currentUser":{"username":"token-owner"}}}"#),
            list_response(Vec::new(), None, Some("token-owner")),
        ]);
        let gitlab = GitLabCli::with_runner(runner.clone());
        gitlab
            .list_page(&scope(), ChangeRequestFilter::Reviewing, None, false, false)
            .await
            .unwrap();
        let requests = runner.requests();
        assert_eq!(
            requests[1].args,
            [
                "api".to_string(),
                "graphql".into(),
                "--hostname".into(),
                "gitlab.com".into(),
                "-f".into(),
                format!("query={GITLAB_VIEWER_QUERY}"),
            ]
        );
        assert_eq!(
            requests[2].args,
            graphql_args(&[
                ("fullPath", "acme/platform/team/api"),
                ("reviewer", "token-owner")
            ])
        );
    }

    #[tokio::test]
    async fn first_pages_are_stored_per_gitlab_account() {
        let dir = tempfile::tempdir().unwrap();
        let runner = FakeProcessRunner::with_responses([
            command_success("devuser\n"),
            list_response(vec![merge_request(1, "devuser")], None, Some("devuser")),
        ]);
        let gitlab = GitLabCli::with_runner(runner.clone())
            .with_hosts(&["git.example.org"])
            .with_store(dir.path());
        assert_eq!(
            gitlab.account_key("GitLab.com").await.as_deref(),
            Some("gitlab:gitlab.com:devuser")
        );
        // Unserved hosts have no account to read.
        assert_eq!(gitlab.account_key("github.com").await, None);
        assert_eq!(gitlab.account_key("gitlab.example.com").await, None);
        let fetched = gitlab
            .list_page(&scope(), ChangeRequestFilter::All, None, false, false)
            .await
            .unwrap();
        // The stored copy is served without asking GitLab again.
        let stored = gitlab
            .list_page(&scope(), ChangeRequestFilter::All, None, false, true)
            .await
            .unwrap();
        assert_eq!(stored.items, fetched.items);
        assert_eq!(runner.requests().len(), 2);
    }

    #[tokio::test]
    async fn failures_are_classified_without_exposing_output() {
        let not_found = || {
            command_success(
                serde_json::to_vec(&json!({"data": {
                    "currentUser": {"username": "devuser"}, "project": null
                }}))
                .unwrap(),
            )
        };
        let signed_out = || {
            command_success(
                serde_json::to_vec(&json!({"data": {"currentUser": null, "project": null}}))
                    .unwrap(),
            )
        };
        let truncated = || {
            Ok(super::super::ProcessOutput {
                success: true,
                stdout: b"{\"data\":".to_vec(),
                stderr: Vec::new(),
                stdout_truncated: true,
            })
        };
        for (response, expected) in [
            (
                Err(ProcessRunError::Spawn(std::io::ErrorKind::NotFound)),
                ChangeRequestError::CliUnavailable,
            ),
            (Err(ProcessRunError::Timeout), ChangeRequestError::Timeout),
            (
                command_failure("glab: Invalid token"),
                ChangeRequestError::Authentication,
            ),
            (
                command_failure("ERROR\n  Unauthenticated.\n"),
                ChangeRequestError::Authentication,
            ),
            (
                command_failure("429 Too Many Requests"),
                ChangeRequestError::RateLimited,
            ),
            (
                command_failure("private-provider-diagnostic"),
                ChangeRequestError::CommandFailed,
            ),
            (signed_out(), ChangeRequestError::Authentication),
            (not_found(), ChangeRequestError::UnsupportedRepository),
            (truncated(), ChangeRequestError::Decode),
            (command_success("not json"), ChangeRequestError::Decode),
        ] {
            let runner = FakeProcessRunner::with_responses([response]);
            let result = GitLabCli::with_runner(runner)
                .list_page(&scope(), ChangeRequestFilter::All, None, false, false)
                .await;
            assert_eq!(result.err(), Some(expected));
        }
    }

    #[tokio::test]
    async fn partial_errors_keep_rows_but_drop_their_metadata() {
        let mut response: serde_json::Value = serde_json::from_slice(
            &list_response(vec![merge_request(1, "devuser")], None, Some("devuser"))
                .unwrap()
                .stdout,
        )
        .unwrap();
        response["errors"] = json!([{"message": "approvalsLeft is unavailable"}]);
        let runner = FakeProcessRunner::with_responses([Ok(super::super::ProcessOutput {
            success: false,
            stdout: serde_json::to_vec(&response).unwrap(),
            stderr: b"glab: approvalsLeft is unavailable".to_vec(),
            stdout_truncated: false,
        })]);
        let page = GitLabCli::with_runner(runner)
            .list_page(&scope(), ChangeRequestFilter::All, None, false, false)
            .await
            .unwrap();
        assert_eq!(page.items[0].ci.state, CiState::Unknown);
        assert_eq!(page.items[0].viewer_did_author, None);
        assert_eq!(page.items[0].viewer_review_requested, None);
    }

    #[tokio::test]
    async fn canonical_scopes_come_from_gitlab_and_detail_stays_in_the_browser() {
        let runner = FakeProcessRunner::with_responses([command_success(
            r#"{"data":{"project":{"fullPath":"acme/platform/team/renamed-api"}}}"#,
        )]);
        let gitlab = GitLabCli::with_runner(runner.clone());
        assert_eq!(
            gitlab.canonical_scope(&scope()).await,
            ChangeRequestScope::from_wire(
                "gitlab.com/acme/platform/team/renamed-api"
            )
        );
        assert_eq!(
            runner.requests()[0].args,
            [
                "api".to_string(),
                "graphql".into(),
                "--hostname".into(),
                "gitlab.com".into(),
                "-f".into(),
                format!("query={GITLAB_PROJECT_QUERY}"),
                "-f".into(),
                "fullPath=acme/platform/team/api".into(),
            ]
        );
        // Neither a GitHub scope nor an unserved host reaches glab.
        for unserved in [
            ChangeRequestScope::from_wire("acme/zeron").unwrap(),
            ChangeRequestScope::from_wire("gitlab.example.com/group/project").unwrap(),
        ] {
            assert_eq!(gitlab.canonical_scope(&unserved).await, None);
            assert_eq!(
                gitlab
                    .list_page(&unserved, ChangeRequestFilter::All, None, false, false)
                    .await
                    .err(),
                Some(ChangeRequestError::UnsupportedRepository)
            );
        }
        let url =
            "https://gitlab.com/acme/platform/team/api/-/merge_requests/441";
        assert!(gitlab.owns_url(url));
        assert_eq!(
            gitlab
                .change_request(url, false, false, None, None)
                .await
                .err(),
            Some(ChangeRequestError::UnsupportedRepository)
        );
        assert_eq!(
            gitlab.post_comment(url, "hello").await.err(),
            Some(ChangeRequestError::UnsupportedRepository)
        );
        assert_eq!(runner.requests().len(), 1);
    }

    #[test]
    fn configured_hosts_come_from_the_hosts_map() {
        let config = "\
# What protocol to use
git_protocol: ssh
host: gitlab.com
hosts:
    # Configuration specific for GitLab instances.
    gitlab.com:
        api_protocol: https
        api_host: gitlab.com
        user: devuser
    git.example.org:
        user: someone
        custom_headers: []
    \"Gitlab.Corp.Example\":
        user: someone
    github.com:
        user: wrong
    -bad:
        user: x
aliases:
    ci.example.com:
";
        assert_eq!(
            config_hosts(config),
            ["gitlab.com", "git.example.org", "gitlab.corp.example"]
        );
        assert!(config_hosts("hosts: {}\n").is_empty());
        assert!(config_hosts("").is_empty());
    }
}
