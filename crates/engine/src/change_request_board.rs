//! Provider routing for the pull request board.
//!
//! The board speaks in scopes (a repository on a host) and change request
//! URLs; an adapter per provider turns those into CLI calls. Routing lives
//! here so RPC handlers never name a provider: the registry reads a wire
//! scope, finds the adapter serving its host, and that adapter alone decides
//! whether the path names one of its repositories.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use zeron_proto::{
    ChangeRequestComment, ChangeRequestFilter, ChangeRequestListItem, ChangeRequestPage,
    ChangeRequestPresentation, ChangeRequestScope,
};

use crate::change_request_store::ChangeRequestStore;
use crate::source_control::{ChangeRequestError, GitHubCli};

/// What the board needs from one provider. Every method validates its own
/// inputs before they reach a CLI, whatever the caller already checked.
#[async_trait]
pub(crate) trait ChangeRequestBoardProvider: Send + Sync {
    /// Stamped on list items and change requests.
    fn provider_id(&self) -> &'static str;

    /// Whether this adapter answers for repositories on `host` (lowercase).
    /// When several do, the one registered first wins.
    fn serves_host(&self, host: &str) -> bool;

    /// `path` on `host` as a scope, when it names one of this provider's
    /// repositories. Only asked for hosts the adapter serves.
    fn scope(&self, host: &str, path: &str) -> Option<ChangeRequestScope>;

    /// Whether `url` names a change request this adapter can open.
    fn owns_url(&self, url: &str) -> bool;

    /// How clients name and show this provider, sent with its pages and
    /// details so no client has to know it.
    fn presentation(&self) -> ChangeRequestPresentation;

    /// Fill a row's presentation fields (its `reference`, avatars) from
    /// what the row already says. Applied to every row served, stored ones
    /// included, so rows stored by an older build read the same.
    fn present(&self, _item: &mut ChangeRequestListItem) {}

    /// The signed-in account the on-disk store keys pages by, read locally.
    /// Each adapter namespaces its own keys. `None` when it can't be told,
    /// and then nothing is stored.
    async fn account_key(&self) -> Option<String>;

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
    /// The built-in adapters, keeping first pages in `data_dir`. Order
    /// settles hosts more than one adapter claims.
    pub(crate) fn new(data_dir: &Path) -> Self {
        Self::with_adapters(vec![Arc::new(GitHubCli::new().with_store(data_dir))])
    }

    fn with_adapters(adapters: Vec<Arc<dyn ChangeRequestBoardProvider>>) -> Self {
        Self { adapters }
    }

    /// Put `adapter` first, replacing any adapter with its id.
    #[cfg(test)]
    pub(crate) fn with_adapter(mut self, adapter: Arc<dyn ChangeRequestBoardProvider>) -> Self {
        self.adapters
            .retain(|existing| existing.provider_id() != adapter.provider_id());
        self.adapters.insert(0, adapter);
        self
    }

    fn for_host(&self, host: &str) -> Option<&dyn ChangeRequestBoardProvider> {
        let host = host.to_ascii_lowercase();
        self.adapters
            .iter()
            .find(|adapter| adapter.serves_host(&host))
            .map(|adapter| adapter.as_ref())
    }

    /// Whether any adapter answers for `host`.
    pub(crate) fn serves_host(&self, host: &str) -> bool {
        self.for_host(host).is_some()
    }

    /// The adapter serving `scope`'s host, if it names that repository.
    fn for_scope(
        &self,
        scope: &ChangeRequestScope,
    ) -> Result<&dyn ChangeRequestBoardProvider, ChangeRequestError> {
        self.for_host(scope.host())
            .filter(|adapter| adapter.scope(scope.host(), scope.path()).is_some())
            .ok_or(ChangeRequestError::UnsupportedRepository)
    }

    fn for_url(&self, url: &str) -> Result<&dyn ChangeRequestBoardProvider, ChangeRequestError> {
        self.adapters
            .iter()
            .find(|adapter| adapter.owns_url(url))
            .map(|adapter| adapter.as_ref())
            .ok_or(ChangeRequestError::UnsupportedRepository)
    }

    /// `path` on `host` as a scope, when the adapter serving that host names
    /// a repository there.
    pub(crate) fn scope(&self, host: &str, path: &str) -> Option<ChangeRequestScope> {
        let host = host.to_ascii_lowercase();
        self.for_host(&host)?.scope(&host, path)
    }

    /// A scope as the wire spells it, when an adapter accepts it.
    pub(crate) fn parse(&self, wire: &str) -> Option<ChangeRequestScope> {
        let scope = ChangeRequestScope::parse(wire)?;
        self.scope(scope.host(), scope.path())
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
        let adapter = self.for_scope(scope)?;
        let mut page = adapter
            .list_page(scope, filter, after, refresh, cached)
            .await?;
        // Rows name the adapter that listed them, whatever it wrote.
        for item in &mut page.items {
            item.provider = adapter.provider_id().into();
            adapter.present(item);
        }
        page.presentation = Some(adapter.presentation());
        Ok(page)
    }

    /// How clients present `scope`'s provider, for a board that has no page
    /// to read it from (its first load failed).
    pub(crate) fn presentation(
        &self,
        scope: &ChangeRequestScope,
    ) -> Option<ChangeRequestPresentation> {
        self.for_scope(scope)
            .ok()
            .map(|adapter| adapter.presentation())
    }

    pub(crate) async fn change_request(
        &self,
        url: &str,
        diff: bool,
        refresh: bool,
        head: Option<&str>,
        base: Option<&str>,
    ) -> Result<serde_json::Value, ChangeRequestError> {
        let adapter = self.for_url(url)?;
        let mut value = adapter
            .change_request(url, diff, refresh, head, base)
            .await?;
        if let Some(detail) = value.as_object_mut().filter(|_| !diff) {
            let presentation = serde_json::to_value(adapter.presentation())
                .map_err(|_| ChangeRequestError::Decode)?;
            detail.insert("presentation".into(), presentation);
        }
        Ok(value)
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
    let repository = scope.to_string();
    let repository = repository.as_str();
    let stored = match store {
        Some(store) if after.is_none() => provider
            .account_key()
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
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::DateTime;
    use zeron_proto::{ChangeRequestFeatures, ChangeRequestMergeability, ChangeRequestState};

    use super::*;

    /// A second provider, registered only in tests and answering from
    /// memory: projects nest under groups on `forge.test`, and its change
    /// requests read `~7`. Nothing it does reaches a process.
    pub(crate) struct Forge {
        hosts: Vec<String>,
        repositories: Mutex<HashMap<String, Vec<ChangeRequestListItem>>>,
        pub(crate) calls: AtomicUsize,
    }

    impl Forge {
        pub(crate) fn new() -> Self {
            Self::serving(&["forge.test"])
        }

        pub(crate) fn serving(hosts: &[&str]) -> Self {
            Self {
                hosts: hosts.iter().map(|host| (*host).to_owned()).collect(),
                repositories: Default::default(),
                calls: AtomicUsize::new(0),
            }
        }

        /// Open change requests `numbers` in `path`, newest first.
        pub(crate) fn with_repository(self, path: &str, numbers: &[u64]) -> Self {
            let items = numbers.iter().map(|number| item(path, *number)).collect();
            self.repositories
                .lock()
                .unwrap()
                .insert(path.to_ascii_lowercase(), items);
            self
        }

        pub(crate) fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    pub(crate) fn item(path: &str, number: u64) -> ChangeRequestListItem {
        ChangeRequestListItem {
            provider: "forge".into(),
            author: Default::default(),
            repository: format!("forge.test/{path}"),
            head_ref_oid: String::new(),
            ci: Default::default(),
            viewer_did_author: None,
            viewer_review_requested: None,
            number,
            title: format!("Change {number}"),
            url: format!("https://forge.test/{path}/-/changes/{number}"),
            state: ChangeRequestState::Open,
            is_draft: false,
            review_decision: Default::default(),
            additions: 1,
            deletions: 1,
            mergeability: ChangeRequestMergeability::Unknown,
            created_at: DateTime::UNIX_EPOCH,
            updated_at: DateTime::UNIX_EPOCH,
            reference: None,
        }
    }

    #[async_trait]
    impl ChangeRequestBoardProvider for Forge {
        fn provider_id(&self) -> &'static str {
            "forge"
        }

        fn serves_host(&self, host: &str) -> bool {
            self.hosts.iter().any(|served| served == host)
        }

        /// Groups nest at most four deep here, tighter than the wire allows.
        fn scope(&self, host: &str, path: &str) -> Option<ChangeRequestScope> {
            (path.split('/').count() <= 5)
                .then(|| ChangeRequestScope::new(host, path))
                .flatten()
        }

        fn owns_url(&self, url: &str) -> bool {
            url.strip_prefix("https://forge.test/")
                .and_then(|rest| rest.split_once("/-/changes/"))
                .is_some_and(|(path, number)| {
                    self.scope("forge.test", path).is_some() && number.parse::<u64>().is_ok()
                })
        }

        fn presentation(&self) -> ChangeRequestPresentation {
            ChangeRequestPresentation {
                provider_name: "Forge".into(),
                cli_name: "Forge CLI".into(),
                cli_command: "forge".into(),
                sign_in_command: "forge login".into(),
                noun: "change".into(),
                media_hosts: vec!["forge.test".into()],
                media_rewrites: Vec::new(),
                features: ChangeRequestFeatures {
                    native_detail: true,
                    comments: false,
                    handoff: false,
                },
            }
        }

        fn present(&self, item: &mut ChangeRequestListItem) {
            item.reference = Some(format!("~{}", item.number));
        }

        async fn account_key(&self) -> Option<String> {
            Some("forge:devuser".into())
        }

        async fn canonical_scope(&self, scope: &ChangeRequestScope) -> Option<ChangeRequestScope> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Some(scope.clone())
        }

        async fn list_page(
            &self,
            scope: &ChangeRequestScope,
            _filter: ChangeRequestFilter,
            _after: Option<&str>,
            _refresh: bool,
            _cached: bool,
        ) -> Result<ChangeRequestPage, ChangeRequestError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let items = self
                .repositories
                .lock()
                .unwrap()
                .get(&scope.path().to_ascii_lowercase())
                .cloned()
                .ok_or(ChangeRequestError::CommandFailed)?;
            Ok(ChangeRequestPage {
                total_count: Some(items.len() as u64),
                items,
                next_cursor: None,
                fetched_at: Some(chrono::Utc::now()),
                presentation: None,
            })
        }

        async fn change_request(
            &self,
            url: &str,
            diff: bool,
            _refresh: bool,
            _head: Option<&str>,
            _base: Option<&str>,
        ) -> Result<serde_json::Value, ChangeRequestError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(if diff {
                serde_json::Value::String(String::new())
            } else {
                serde_json::json!({ "url": url })
            })
        }

        async fn post_comment(
            &self,
            _url: &str,
            body: &str,
        ) -> Result<ChangeRequestComment, ChangeRequestError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ChangeRequestComment {
                body: body.into(),
                viewer_did_author: true,
                ..Default::default()
            })
        }
    }

    /// GitHub, unable to start a process, then the forge.
    fn registry(forge: Forge) -> (ChangeRequestBoardProviders, Arc<Forge>) {
        let forge = Arc::new(forge);
        let providers = ChangeRequestBoardProviders::with_adapters(vec![
            Arc::new(GitHubCli::offline()),
            forge.clone(),
        ]);
        (providers, forge)
    }

    fn adapter_for(providers: &ChangeRequestBoardProviders, wire: &str) -> Option<&'static str> {
        let scope = providers.parse(wire)?;
        providers
            .for_scope(&scope)
            .ok()
            .map(|adapter| adapter.provider_id())
    }

    #[test]
    fn wire_scopes_route_by_host_and_each_adapter_names_its_paths() {
        let (providers, _) = registry(Forge::new());
        for (wire, provider, canonical) in [
            ("acme/zeron", Some("github"), "acme/zeron"),
            ("GitHub.com/acme/zeron", Some("github"), "acme/zeron"),
            (
                "Forge.test/team/platform/app",
                Some("forge"),
                "forge.test/team/platform/app",
            ),
            (
                "forge.test/a/b/c/d/e",
                Some("forge"),
                "forge.test/a/b/c/d/e",
            ),
            // GitHub repositories are never nested; the forge stops at four
            // groups, tighter than the wire.
            ("github.com/acme/zeron/extra", None, ""),
            ("forge.test/a/b/c/d/e/f", None, ""),
            // No adapter serves this host.
            ("other.test/team/app", None, ""),
            ("not a scope", None, ""),
        ] {
            assert_eq!(adapter_for(&providers, wire), provider, "{wire}");
            if provider.is_some() {
                assert_eq!(providers.parse(wire).unwrap().to_string(), canonical);
            }
        }
        assert_eq!(
            providers.scope("FORGE.test", "team/app"),
            ChangeRequestScope::parse("forge.test/team/app")
        );
        assert!(providers.serves_host("forge.test") && providers.serves_host("GitHub.com"));
        assert!(!providers.serves_host("other.test"));
    }

    #[test]
    fn hosts_more_than_one_adapter_claims_go_to_the_first_registered() {
        let claiming = || Arc::new(Forge::serving(&["forge.test", "github.com"]));
        let github_first = ChangeRequestBoardProviders::with_adapters(vec![
            Arc::new(GitHubCli::offline()),
            claiming(),
        ]);
        assert_eq!(adapter_for(&github_first, "acme/zeron"), Some("github"));
        let forge_first = ChangeRequestBoardProviders::with_adapters(vec![
            claiming(),
            Arc::new(GitHubCli::offline()),
        ]);
        assert_eq!(adapter_for(&forge_first, "acme/zeron"), Some("forge"));
        assert_eq!(
            adapter_for(&forge_first, "forge.test/team/app"),
            Some("forge")
        );
    }

    #[tokio::test]
    async fn unserved_scopes_and_urls_fail_before_any_adapter_call() {
        let (providers, forge) = registry(Forge::new().with_repository("team/app", &[7]));
        let unserved = ChangeRequestScope::new("other.test", "team/app").unwrap();
        let nested_github = ChangeRequestScope::new("github.com", "acme/zeron/extra").unwrap();
        for scope in [&unserved, &nested_github] {
            assert_eq!(
                providers
                    .list_page(scope, ChangeRequestFilter::All, None, false, false)
                    .await
                    .err(),
                Some(ChangeRequestError::UnsupportedRepository),
                "{scope}"
            );
            assert_eq!(providers.canonical_scope(scope).await, None);
        }
        for url in [
            "https://other.test/team/app/-/changes/7",
            "https://forge.test/a/b/c/d/e/f/-/changes/7",
            "not a url",
        ] {
            assert_eq!(
                providers
                    .change_request(url, false, false, None, None)
                    .await
                    .err(),
                Some(ChangeRequestError::UnsupportedRepository),
                "{url}"
            );
            assert_eq!(
                providers.post_comment(url, "hello").await.err(),
                Some(ChangeRequestError::UnsupportedRepository)
            );
        }
        assert_eq!(forge.calls(), 0);
        // A served scope reaches its adapter.
        let scope = providers.parse("forge.test/team/app").unwrap();
        let page = providers
            .list_page(&scope, ChangeRequestFilter::All, None, false, false)
            .await
            .unwrap();
        assert_eq!(page.items[0].url, "https://forge.test/team/app/-/changes/7");
        assert_eq!(forge.calls(), 1);
    }

    #[tokio::test]
    async fn pages_and_details_carry_their_adapters_presentation() {
        let (providers, _) = registry(Forge::new().with_repository("team/platform/app", &[7]));
        let scope = providers.parse("forge.test/team/platform/app").unwrap();
        let page = providers
            .list_page(&scope, ChangeRequestFilter::All, None, false, false)
            .await
            .unwrap();
        assert_eq!(page.items[0].reference.as_deref(), Some("~7"));
        assert_eq!(page.items[0].provider, "forge");
        let presentation = page.presentation.unwrap();
        assert_eq!(presentation.provider_name, "Forge");
        assert!(presentation.features.native_detail && !presentation.features.comments);
        assert_eq!(providers.presentation(&scope), Some(presentation.clone()));
        let github = providers.parse("acme/zeron").unwrap();
        assert_eq!(
            providers
                .presentation(&github)
                .map(|presentation| presentation.provider_name),
            Some("GitHub".into())
        );
        let url = "https://forge.test/team/platform/app/-/changes/7";
        let detail: zeron_proto::ChangeRequestDetail = serde_json::from_value(
            providers
                .change_request(url, false, false, None, None)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(detail.presentation, Some(presentation));
        // A patch is text; it carries nothing else.
        assert_eq!(
            providers.change_request(url, true, false, None, None).await,
            Ok(serde_json::Value::String(String::new()))
        );
    }
}
