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
use crate::source_control::{ChangeRequestError, GitHubCli, valid_pr_repository};

pub(crate) const GITHUB_PROVIDER: &str = "github";
pub(crate) const GITHUB_HOST: &str = "github.com";

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
    /// The board's wire form predates other hosts: a bare `owner/repo` always
    /// means github.com, and nothing else is accepted.
    pub(crate) fn from_wire(repository: &str) -> Option<Self> {
        valid_pr_repository(repository).then(|| Self {
            provider: GITHUB_PROVIDER,
            host: GITHUB_HOST.into(),
            path: repository.into(),
        })
    }

    pub(crate) fn to_wire(&self) -> String {
        self.path.clone()
    }

    /// Hosts and repository paths compare without case, as GitHub and GitLab
    /// both resolve them.
    pub(crate) fn same_repository(&self, other: &Self) -> bool {
        self.provider == other.provider
            && self.host.eq_ignore_ascii_case(&other.host)
            && self.path.eq_ignore_ascii_case(&other.path)
    }
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
    /// The built-in adapters, keeping first pages in `data_dir`.
    pub(crate) fn new(data_dir: &Path) -> Self {
        Self::with_adapters(vec![Arc::new(GitHubCli::new().with_store(data_dir))])
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

    /// `path` on `host` as a scope, when an adapter serves that host.
    pub(crate) fn scope(&self, host: &str, path: &str) -> Option<ChangeRequestScope> {
        self.for_host(host).map(|adapter| ChangeRequestScope {
            provider: adapter.provider_id(),
            host: host.to_ascii_lowercase(),
            path: path.into(),
        })
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
    repository: &str,
    filter: ChangeRequestFilter,
    after: Option<&str>,
    refresh: bool,
    cached: bool,
    fetch: impl Future<Output = Result<ChangeRequestPage, ChangeRequestError>>,
) -> Result<ChangeRequestPage, ChangeRequestError>
where
    P: ChangeRequestBoardProvider + ?Sized,
{
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
        // The wire keeps today's strict owner/repo form in this version.
        assert_eq!(ChangeRequestScope::from_wire(&scope.path), None);
    }
}
