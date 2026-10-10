//! Provider routing for the pull request board.
//!
//! The board speaks in scopes (which repository, on which host) and change
//! request URLs; an adapter per provider turns those into CLI calls. Routing
//! lives here so RPC handlers never name a provider, and an adapter for
//! another host slots in beside GitHub without touching them.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use zeron_proto::{ChangeRequestComment, ChangeRequestFilter, ChangeRequestPage};

use crate::change_request_store::ChangeRequestStore;
use crate::source_control::{ChangeRequestError, GitHubCli, GitLabCli, valid_pr_repository};

pub(crate) const GITHUB_PROVIDER: &str = "github";
pub(crate) const GITHUB_HOST: &str = "github.com";
pub(crate) const GITLAB_PROVIDER: &str = "gitlab";
pub(crate) const GITLAB_HOST: &str = "gitlab.com";

/// One repository on one provider host. `path` is the repository's full path
/// as the host names it: `owner/repo` on GitHub, while GitLab nests groups
/// (`group/subgroup/project`), so it is never split into owner and name here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChangeRequestScope {
    pub provider: &'static str,
    /// Lowercase.
    pub host: String,
    pub path: String,
}

impl ChangeRequestScope {
    /// `path` on `host`, when `provider` can name it there: `owner/repo` on
    /// github.com, or a project under at least one group on a GitLab host.
    pub(crate) fn new(provider: &'static str, host: &str, path: &str) -> Option<Self> {
        let host = host.to_ascii_lowercase();
        let valid = match provider {
            GITHUB_PROVIDER => host == GITHUB_HOST && valid_pr_repository(path),
            GITLAB_PROVIDER => host != GITHUB_HOST && valid_host(&host) && valid_gitlab_path(path),
            _ => false,
        };
        valid.then(|| Self {
            provider,
            host,
            path: path.into(),
        })
    }

    /// The board's wire form. A bare `owner/repo` means github.com, as it
    /// always has, so older peers keep working. Any other host travels
    /// qualified, `host/group/.../project`, and is GitLab's; engines that
    /// accept that form advertise `change-requests-gitlab-v1`.
    pub(crate) fn from_wire(repository: &str) -> Option<Self> {
        if valid_pr_repository(repository) {
            return Self::new(GITHUB_PROVIDER, GITHUB_HOST, repository);
        }
        let (host, path) = repository.split_once('/')?;
        Self::new(GITLAB_PROVIDER, host, path)
    }

    pub(crate) fn to_wire(&self) -> String {
        if self.provider == GITHUB_PROVIDER {
            self.path.clone()
        } else {
            format!("{}/{}", self.host, self.path)
        }
    }

    /// Hosts and repository paths compare without case, as GitHub and GitLab
    /// both resolve them.
    pub(crate) fn same_repository(&self, other: &Self) -> bool {
        self.provider == other.provider
            && self.host.eq_ignore_ascii_case(&other.host)
            && self.path.eq_ignore_ascii_case(&other.path)
    }
}

/// A hostname as remotes and URLs carry it, never an option or a user. A dot
/// is required, so a qualified scope can't be mistaken for `owner/repo`.
pub(crate) fn valid_host(host: &str) -> bool {
    (1..=253).contains(&host.len())
        && host.contains('.')
        && !host.starts_with(['-', '.'])
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
}

/// A GitLab project's full path: a project under one or more groups, which
/// GitLab nests at most 20 deep.
pub(crate) fn valid_gitlab_path(path: &str) -> bool {
    let parts: Vec<_> = path.split('/').collect();
    (2..=21).contains(&parts.len())
        && path.len() <= 512
        && parts.iter().all(|part| {
            !part.is_empty()
                && *part != "."
                && *part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        })
}

/// What the board needs from one provider. Every method validates its own
/// inputs before they reach a CLI, whatever the caller already checked.
#[async_trait]
pub(crate) trait ChangeRequestBoardProvider: Send + Sync {
    /// Stamped on scopes and list items.
    fn provider_id(&self) -> &'static str;

    /// Whether this adapter answers for repositories on `host`.
    fn serves_host(&self, host: &str) -> bool;

    /// Whether `url` names a change request this adapter can open.
    fn owns_url(&self, url: &str) -> bool;

    /// The account signed in on `host` that the on-disk store keys pages
    /// by, read locally. Each adapter namespaces its own keys. `None` when it
    /// can't be told, and then nothing is stored.
    async fn account_key(&self, host: &str) -> Option<String>;

    /// The provider's current name for `scope`, following renames.
    async fn canonical_scope(&self, scope: &ChangeRequestScope) -> Option<ChangeRequestScope>;

    async fn list_page(
        &self,
        scope: &ChangeRequestScope,
        filter: ChangeRequestFilter,
        after: Option<&str>,
        refresh: bool,
        cached: bool,
    ) -> Result<ChangeRequestPage, ChangeRequestError>;

    /// The detail, or with `diff` the patch, as the wire value.
    async fn change_request(
        &self,
        url: &str,
        diff: bool,
        refresh: bool,
        head: Option<&str>,
        base: Option<&str>,
    ) -> Result<serde_json::Value, ChangeRequestError>;

    async fn post_comment(
        &self,
        url: &str,
        body: &str,
    ) -> Result<ChangeRequestComment, ChangeRequestError>;
}

/// The board's adapters, picked by host or URL. Trait objects rather than an
/// enum: adapters are injected in tests like the process runner, and a
/// self-hosted provider is data (its host), not a new variant.
#[derive(Clone)]
pub(crate) struct ChangeRequestBoardProviders {
    adapters: Vec<Arc<dyn ChangeRequestBoardProvider>>,
}

impl ChangeRequestBoardProviders {
    /// The built-in adapters, keeping first pages in `data_dir`.
    pub(crate) fn new(data_dir: &Path) -> Self {
        Self::with_adapters(vec![
            Arc::new(GitHubCli::new().with_store(data_dir)),
            Arc::new(GitLabCli::new().with_store(data_dir)),
        ])
    }

    fn with_adapters(adapters: Vec<Arc<dyn ChangeRequestBoardProvider>>) -> Self {
        Self { adapters }
    }

    /// Replace the GitHub adapter.
    #[cfg(test)]
    pub(crate) fn with_github(mut self, github: GitHubCli) -> Self {
        self.adapters
            .retain(|adapter| adapter.provider_id() != GITHUB_PROVIDER);
        self.adapters.insert(0, Arc::new(github));
        self
    }

    /// Replace the GitLab adapter.
    #[cfg(test)]
    pub(crate) fn with_gitlab(mut self, gitlab: GitLabCli) -> Self {
        self.adapters
            .retain(|adapter| adapter.provider_id() != GITLAB_PROVIDER);
        self.adapters.push(Arc::new(gitlab));
        self
    }

    pub(crate) fn for_host(&self, host: &str) -> Option<&dyn ChangeRequestBoardProvider> {
        self.adapters
            .iter()
            .find(|adapter| adapter.serves_host(host))
            .map(|adapter| adapter.as_ref())
    }

    fn for_scope(
        &self,
        scope: &ChangeRequestScope,
    ) -> Result<&dyn ChangeRequestBoardProvider, ChangeRequestError> {
        self.for_host(&scope.host)
            .filter(|adapter| adapter.provider_id() == scope.provider)
            .ok_or(ChangeRequestError::UnsupportedRepository)
    }

    fn for_url(&self, url: &str) -> Result<&dyn ChangeRequestBoardProvider, ChangeRequestError> {
        self.adapters
            .iter()
            .find(|adapter| adapter.owns_url(url))
            .map(|adapter| adapter.as_ref())
            .ok_or(ChangeRequestError::UnsupportedRepository)
    }

    /// `path` on `host` as a scope, when an adapter serves that host and
    /// can name the path.
    pub(crate) fn scope(&self, host: &str, path: &str) -> Option<ChangeRequestScope> {
        self.for_host(host)
            .and_then(|adapter| ChangeRequestScope::new(adapter.provider_id(), host, path))
    }

    pub(crate) async fn canonical_scope(
        &self,
        scope: &ChangeRequestScope,
    ) -> Option<ChangeRequestScope> {
        self.for_scope(scope).ok()?.canonical_scope(scope).await
    }

    pub(crate) async fn list_page(
        &self,
        scope: &ChangeRequestScope,
        filter: ChangeRequestFilter,
        after: Option<&str>,
        refresh: bool,
        cached: bool,
    ) -> Result<ChangeRequestPage, ChangeRequestError> {
        self.for_scope(scope)?
            .list_page(scope, filter, after, refresh, cached)
            .await
    }

    pub(crate) async fn change_request(
        &self,
        url: &str,
        diff: bool,
        refresh: bool,
        head: Option<&str>,
        base: Option<&str>,
    ) -> Result<serde_json::Value, ChangeRequestError> {
        self.for_url(url)?
            .change_request(url, diff, refresh, head, base)
            .await
    }

    pub(crate) async fn post_comment(
        &self,
        url: &str,
        body: &str,
    ) -> Result<ChangeRequestComment, ChangeRequestError> {
        self.for_url(url)?.post_comment(url, body).await
    }
}

/// A list page through the on-disk store, shared so every adapter keys it the
/// same way. Only first pages are stored. `cached` asks for the stored copy,
/// whatever its age, so the board can show it at once and refresh behind it;
/// without one, `fetch` runs as usual. Every first page fetched is stored.
#[allow(clippy::too_many_arguments)] // one board query, spelled out as the RPC sends it
pub(crate) async fn stored_page<P>(
    provider: &P,
    store: Option<&ChangeRequestStore>,
    scope: &ChangeRequestScope,
    filter: ChangeRequestFilter,
    after: Option<&str>,
    refresh: bool,
    cached: bool,
    fetch: impl Future<Output = Result<ChangeRequestPage, ChangeRequestError>>,
) -> Result<ChangeRequestPage, ChangeRequestError>
where
    P: ChangeRequestBoardProvider + ?Sized,
{
    let repository = scope.to_wire();
    let repository = repository.as_str();
    let stored = match store {
        Some(store) if after.is_none() => provider
            .account_key(&scope.host)
            .await
            .map(|account| (store.clone(), account)),
        _ => None,
    };
    if cached
        && !refresh
        && let Some((store, account)) = &stored
        && let Some(page) = store.load(account, repository, filter).await
    {
        return Ok(page);
    }
    let page = fetch.await?;
    if let Some((store, account)) = stored {
        store.save(&account, repository, filter, &page).await;
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn github_only() -> ChangeRequestBoardProviders {
        ChangeRequestBoardProviders::with_adapters(vec![Arc::new(GitHubCli::new())])
    }

    #[test]
    fn hosts_route_to_their_adapter() {
        let providers = github_only();
        for host in ["github.com", "GitHub.com"] {
            assert_eq!(
                providers
                    .for_host(host)
                    .map(|adapter| adapter.provider_id()),
                Some(GITHUB_PROVIDER),
                "{host}"
            );
        }
        // Enterprise and other providers have no adapter yet.
        for host in ["gitlab.com", "github.example.com", ""] {
            assert!(providers.for_host(host).is_none(), "{host}");
        }
        assert_eq!(
            providers.scope("GitHub.com", "acme/zeron"),
            ChangeRequestScope::from_wire("acme/zeron")
        );
        assert_eq!(providers.scope("gitlab.com", "acme/zeron"), None);
    }

    #[test]
    fn urls_route_to_the_adapter_that_owns_them() {
        let providers = github_only();
        assert_eq!(
            providers
                .for_url("https://github.com/acme/zeron/pull/12")
                .map(|adapter| adapter.provider_id()),
            Ok(GITHUB_PROVIDER)
        );
        for url in [
            "https://gitlab.com/acme/zeron/-/merge_requests/12",
            "https://github.example.com/acme/zeron/pull/12",
            "not a url",
        ] {
            assert_eq!(
                providers.for_url(url).err(),
                Some(ChangeRequestError::UnsupportedRepository),
                "{url}"
            );
        }
    }

    #[tokio::test]
    async fn unserved_scopes_and_urls_fail_before_any_provider_call() {
        // The real `gh` would run if routing reached the adapter.
        let providers = github_only();
        let nested = ChangeRequestScope {
            provider: "gitlab",
            host: "gitlab.com".into(),
            path: "acme/platform/team/api".into(),
        };
        assert_eq!(
            providers
                .list_page(&nested, ChangeRequestFilter::All, None, false, false)
                .await
                .err(),
            Some(ChangeRequestError::UnsupportedRepository)
        );
        // A scope claiming another provider on a served host is refused too.
        let mislabelled = ChangeRequestScope {
            provider: "gitlab",
            ..ChangeRequestScope::from_wire("acme/zeron").unwrap()
        };
        assert_eq!(
            providers
                .list_page(&mislabelled, ChangeRequestFilter::All, None, false, false)
                .await
                .err(),
            Some(ChangeRequestError::UnsupportedRepository)
        );
        assert_eq!(providers.canonical_scope(&nested).await, None);
        let url = "https://gitlab.com/acme/zeron/-/merge_requests/12";
        assert_eq!(
            providers
                .change_request(url, false, false, None, None)
                .await
                .err(),
            Some(ChangeRequestError::UnsupportedRepository)
        );
        assert_eq!(
            providers.post_comment(url, "hello").await.err(),
            Some(ChangeRequestError::UnsupportedRepository)
        );
    }

    #[test]
    fn wire_repositories_are_github_scopes() {
        let scope = ChangeRequestScope::from_wire("Acme/zeron.app").unwrap();
        assert_eq!(scope.provider, GITHUB_PROVIDER);
        assert_eq!(scope.host, GITHUB_HOST);
        assert_eq!(scope.path, "Acme/zeron.app");
        assert_eq!(scope.to_wire(), "Acme/zeron.app");
        assert_eq!(
            ChangeRequestScope::from_wire(&scope.to_wire()),
            Some(scope.clone())
        );
        assert!(scope.same_repository(&ChangeRequestScope::from_wire("acme/ZERON.app").unwrap()));
        for repository in [
            "acme",
            "acme/zeron/extra",
            "repo:acme/zeron",
            "../zeron",
            "",
        ] {
            assert_eq!(
                ChangeRequestScope::from_wire(repository),
                None,
                "{repository}"
            );
        }
    }

    #[test]
    fn nested_paths_are_representable() {
        let scope = ChangeRequestScope {
            provider: "gitlab",
            host: "gitlab.com".into(),
            path: "acme/platform/team/api".into(),
        };
        assert_eq!(scope.path.split('/').count(), 4);
        assert!(scope.same_repository(&ChangeRequestScope {
            path: "Acme/Platform/team/api".into(),
            ..scope.clone()
        }));
        assert!(!scope.same_repository(&ChangeRequestScope {
            provider: GITHUB_PROVIDER,
            ..scope.clone()
        }));
        // A bare path names no host, so it is never a GitLab scope.
        assert_eq!(ChangeRequestScope::from_wire(&scope.path), None);
    }

    #[test]
    fn gitlab_scopes_travel_host_qualified() {
        let wire = "GitLab.com/acme/platform/team/api";
        let scope = ChangeRequestScope::from_wire(wire).unwrap();
        assert_eq!(scope.provider, GITLAB_PROVIDER);
        assert_eq!(scope.host, "gitlab.com");
        assert_eq!(scope.path, "acme/platform/team/api");
        assert_eq!(
            scope.to_wire(),
            "gitlab.com/acme/platform/team/api"
        );
        assert_eq!(ChangeRequestScope::from_wire(&scope.to_wire()), Some(scope));
        assert_eq!(
            ChangeRequestScope::from_wire("git.example.org/group/project")
                .map(|scope| (scope.host, scope.path)),
            Some(("git.example.org".into(), "group/project".into()))
        );
        for repository in [
            // A host needs a dot, so three bare segments stay invalid.
            "acme/zeron/extra",
            // GitHub keeps its bare owner/repo form.
            "github.com/acme/zeron",
            "gitlab.com/group//project",
            "gitlab.com/group/../project",
            "-gitlab.com/group/project",
            "gitlab.com:22/group/project",
            "user@gitlab.com/group/project",
            "gitlab.com/group/project?x",
            "gitlab.com/group/pro ject",
        ] {
            assert_eq!(
                ChangeRequestScope::from_wire(repository),
                None,
                "{repository}"
            );
        }
        let deep = format!("gitlab.com/{}project", "group/".repeat(21));
        assert_eq!(ChangeRequestScope::from_wire(&deep), None);
        // Two segments are always GitHub's owner/repo, as before.
        assert_eq!(
            ChangeRequestScope::from_wire("gitlab.com/project").map(|scope| scope.provider),
            Some(GITHUB_PROVIDER)
        );
    }

    #[test]
    fn gitlab_hosts_and_urls_route_to_the_gitlab_adapter() {
        let providers =
            github_only().with_gitlab(GitLabCli::new().with_hosts(&["git.example.org"]));
        for host in ["gitlab.com", "GitLab.com", "git.example.org"] {
            assert_eq!(
                providers
                    .for_host(host)
                    .map(|adapter| adapter.provider_id()),
                Some(GITLAB_PROVIDER),
                "{host}"
            );
        }
        assert_eq!(
            providers
                .for_host("github.com")
                .map(|adapter| adapter.provider_id()),
            Some(GITHUB_PROVIDER)
        );
        for host in ["gitlab.example.com", "example.org"] {
            assert!(providers.for_host(host).is_none(), "{host}");
        }
        assert_eq!(
            providers.scope("gitlab.com", "acme/platform/team/api"),
            ChangeRequestScope::from_wire("gitlab.com/acme/platform/team/api")
        );
        // Each provider names paths its own way.
        assert_eq!(providers.scope("github.com", "acme/zeron/extra"), None);
        assert_eq!(providers.scope("gitlab.com", "project"), None);
        for url in [
            "https://gitlab.com/acme/platform/team/api/-/merge_requests/441",
            "https://git.example.org/group/project/-/merge_requests/1",
        ] {
            assert_eq!(
                providers.for_url(url).map(|adapter| adapter.provider_id()),
                Ok(GITLAB_PROVIDER),
                "{url}"
            );
        }
        assert_eq!(
            providers
                .for_url("https://github.com/acme/zeron/pull/12")
                .map(|adapter| adapter.provider_id()),
            Ok(GITHUB_PROVIDER)
        );
        for url in [
            "https://gitlab.example.com/group/project/-/merge_requests/1",
            "http://gitlab.com/group/project/-/merge_requests/1",
            "https://user@gitlab.com/group/project/-/merge_requests/1",
            "https://gitlab.com:8443/group/project/-/merge_requests/1",
            "https://gitlab.com/project/-/merge_requests/1",
            "https://gitlab.com/group/project/-/merge_requests/0",
            "https://gitlab.com/group/project/-/merge_requests/1/diffs",
            "https://gitlab.com/group/project/-/issues/1",
            "https://gitlab.com/group/project/merge_requests/1",
        ] {
            assert_eq!(
                providers.for_url(url).err(),
                Some(ChangeRequestError::UnsupportedRepository),
                "{url}"
            );
        }
    }
}
