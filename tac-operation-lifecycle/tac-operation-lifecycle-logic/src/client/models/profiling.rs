use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct StageProfilingApiResponse {
    pub response: HashMap<String, OperationData>,
}

#[derive(Debug, Deserialize)]
pub struct OperationData {
    pub operationType: OperationType,
    #[serde(flatten)]
    pub stages: HashMap<StageType, Stage>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationType {
    PENDING,
    TacTonTac,
    TacTon,
    TonTac,
    Rollback,
    Unknown,
    #[serde(other)]
    ErrorType
}

#[derive(Debug, Deserialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum StageType {
    CollectedInTAC,
    IncludedInTACConsensus,
    ExecutedInTAC,
    CollectedInTON,
    IncludedInTONConsensus,
    ExecutedInTON,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stage {
    pub exists: bool,
    pub stage_data: Option<StageData>,
}

#[derive(Debug, Deserialize)]
pub struct StageData {
    pub success: bool,
    pub timestamp: u64,
    pub transactions: Vec<Transaction>,
    pub note: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BlockchainType {
    TAC,
    TON,
    #[serde(other)]
    UNKNOWN,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub hash: String,
    pub blockchain_type: BlockchainType,
}
