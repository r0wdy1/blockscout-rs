use std::time;

use serde::Deserialize;
use serde_with::serde_as;
#[serde_as]
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IndexerSettings {
    pub concurrency: u32,
    #[serde(default = "default_restart_delay")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub restart_delay: time::Duration,
    #[serde(default = "default_polling_interval")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub polling_interval: time::Duration,
    #[serde(default = "default_retry_interval")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub retry_interval: time::Duration,
    #[serde(default = "default_catchup_interval")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub catchup_interval: time::Duration,
    #[serde(default = "default_start_timestamp")]
    pub start_timestamp: u64,
}

fn default_start_timestamp() -> u64 {
    0
}

fn default_polling_interval() -> time::Duration {
    time::Duration::from_secs(0)
}

fn default_retry_interval() -> time::Duration {
    time::Duration::from_secs(180)
}

fn default_restart_delay() -> time::Duration {
    time::Duration::from_secs(60)
}

fn default_catchup_interval() -> time::Duration {
    time::Duration::from_secs(5)
}

impl Default for IndexerSettings {
    fn default() -> Self {
        Self {
            concurrency: 2,
            restart_delay: default_restart_delay(),
            polling_interval: default_polling_interval(),
            retry_interval: default_retry_interval(),
            catchup_interval: default_catchup_interval(),
            start_timestamp: default_start_timestamp(),
        }
    }
}
