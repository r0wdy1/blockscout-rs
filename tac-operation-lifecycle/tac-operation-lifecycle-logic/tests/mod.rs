#[cfg(test)]
mod tests {
    use chrono::Utc;
    use futures::stream::{select_with_strategy, PollNext};
    use futures::StreamExt;
    use std::time::Duration;
    use tac_operation_lifecycle_entity::{interval, operation, sea_orm_active_enums::StatusEnum};
    use tac_operation_lifecycle_logic::{
        indexer::{Job, OperationJob},
        IndexerJob,
    };

    fn prio_left(_: &mut ()) -> PollNext {
        PollNext::Left
    }
    #[tokio::test]
    async fn test_stream_prioirities() { 

        let mut operation_id = 0;
        let mut interval_id = 0;

        let stage_profiling_stream = Box::pin(async_stream::stream! {
            loop {
                yield IndexerJob::Operation(OperationJob { operation: operation::Model {
                    id: format!("{}", operation_id),
                    op_type: Some("1".to_string()),
                    timestamp: Utc::now().naive_utc(),
                    next_retry: None,
                    status: StatusEnum::Pending,
                    retry_count: 0,
                    inserted_at: Utc::now().naive_utc(),
                    updated_at: Utc::now().naive_utc(),
                } });
                tokio::time::sleep(Duration::from_millis(100)).await;
                operation_id += 1;
            }
        });

        let new_operations_stream = Box::pin(async_stream::stream! {
            loop {
                yield IndexerJob::Interval(Job { interval: interval::Model {
                    id: interval_id,
                    start: Utc::now().naive_utc(),
                    finish: Utc::now().naive_utc(),
                    status: StatusEnum::Pending,
                    inserted_at: Utc::now().naive_utc(),
                    updated_at: Utc::now().naive_utc(),
                    next_retry: None,
                    retry_count: 0,
                } });
                interval_id += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
                
            }
        });

        let combined_stream =
            select_with_strategy(new_operations_stream, stage_profiling_stream, prio_left);


            combined_stream.for_each_concurrent(
                std::thread::available_parallelism().unwrap().get(),
                |job| {
                    tokio::spawn(async move {
                        match job {
                            IndexerJob::Operation(operation_job) => {
                                // Fast task
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                println!("Fast: {}", operation_job.operation.id);
                            }
                            IndexerJob::Interval(interval_job) => {
                                // Slow task
                                tokio::time::sleep(Duration::from_secs(10)).await;
                                println!("Slow: {}", interval_job.interval.id);
                            }
                        }
                    });
                    futures::future::ready(())
                }
            ).await;
    }
}
