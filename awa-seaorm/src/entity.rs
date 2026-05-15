use sea_orm::{DeriveActiveEnum, EnumIter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "job_state",
    rename_all = "snake_case"
)]
pub enum JobState {
    Scheduled,
    Available,
    Running,
    Completed,
    Retryable,
    Failed,
    Cancelled,
    WaitingExternal,
}

impl From<awa::JobState> for JobState {
    fn from(value: awa::JobState) -> Self {
        match value {
            awa::JobState::Scheduled => Self::Scheduled,
            awa::JobState::Available => Self::Available,
            awa::JobState::Running => Self::Running,
            awa::JobState::Completed => Self::Completed,
            awa::JobState::Retryable => Self::Retryable,
            awa::JobState::Failed => Self::Failed,
            awa::JobState::Cancelled => Self::Cancelled,
            awa::JobState::WaitingExternal => Self::WaitingExternal,
        }
    }
}

impl From<JobState> for awa::JobState {
    fn from(value: JobState) -> Self {
        match value {
            JobState::Scheduled => Self::Scheduled,
            JobState::Available => Self::Available,
            JobState::Running => Self::Running,
            JobState::Completed => Self::Completed,
            JobState::Retryable => Self::Retryable,
            JobState::Failed => Self::Failed,
            JobState::Cancelled => Self::Cancelled,
            JobState::WaitingExternal => Self::WaitingExternal,
        }
    }
}

pub mod jobs {
    use super::JobState;
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "awa", table_name = "jobs")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: i64,
        pub kind: String,
        pub queue: String,
        pub args: Json,
        pub state: JobState,
        pub priority: i16,
        pub attempt: i16,
        pub max_attempts: i16,
        pub run_at: DateTimeUtc,
        pub heartbeat_at: Option<DateTimeUtc>,
        pub deadline_at: Option<DateTimeUtc>,
        pub attempted_at: Option<DateTimeUtc>,
        pub finalized_at: Option<DateTimeUtc>,
        pub created_at: DateTimeUtc,
        pub errors: Option<Vec<Json>>,
        pub metadata: Json,
        pub tags: Vec<String>,
        pub unique_key: Option<Vec<u8>>,
        #[sea_orm(ignore)]
        pub unique_states: Option<u8>,
        pub callback_id: Option<Uuid>,
        pub callback_timeout_at: Option<DateTimeUtc>,
        pub callback_filter: Option<String>,
        pub callback_on_complete: Option<String>,
        pub callback_on_fail: Option<String>,
        pub callback_transform: Option<String>,
        pub run_lease: i64,
        pub progress: Option<Json>,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod queue_meta {
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(schema_name = "awa", table_name = "queue_meta")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub queue: String,
        pub paused: bool,
        pub paused_at: Option<DateTimeUtc>,
        pub paused_by: Option<String>,
        pub enqueue_shards: i16,
    }

    impl ActiveModelBehavior for ActiveModel {}
}
