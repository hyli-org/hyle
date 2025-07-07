-- Add migration script here
CREATE TABLE blocks (
    hash TEXT PRIMARY KEY,          -- Corresponds to BlockHash 
    parent_hash TEXT NOT NULL,      -- Parent block hash (BlockHash)
    height BIGINT NOT NULL,         -- Corresponds to BlockHeight (u64)
    timestamp TIMESTAMP(3) NOT NULL,   -- UNIX timestamp (u64)
    total_txs BIGINT NOT NULL,         -- Total number of transactions in the block
    UNIQUE (height),                -- Ensure each block height is unique
    CHECK (length(hash) = 64),      -- Ensure the hash is exactly 64
    CHECK (height >= 0)             -- Ensure the height is positive
);

-- Add data_proposals table to store metadata for all data proposals
CREATE TABLE data_proposals (
    hash TEXT PRIMARY KEY,                                        -- The data proposal hash
    parent_hash TEXT,                                             -- Parent data proposal hash (nullable)
    lane_id TEXT NOT NULL,                                        -- Lane that created this proposal
    tx_count INT NOT NULL,                                        -- Number of transactions
    estimated_size BIGINT NOT NULL,                               -- Estimated size in bytes
    block_hash TEXT NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE,  -- Block containing this DP
    block_height BIGINT NOT NULL,                                 -- Height of the block
    created_at TIMESTAMP(3) NOT NULL,                             -- Block timestamp as creation time

    -- CHECK (length(hash) = 64), -- First DP hash of lane is pubkey, so longer than 64
    CHECK (tx_count >= 0),
    CHECK (estimated_size >= 0),
    CHECK (block_height >= 0),
    CHECK (length(lane_id) = 96)
);

CREATE TYPE transaction_type AS ENUM ('blob_transaction', 'proof_transaction', 'stake');
CREATE TYPE transaction_status AS ENUM ('data_proposal_created', 'waiting_dissemination','success', 'failure', 'sequenced', 'timed_out');

CREATE TABLE transactions (
    dp_hash TEXT,                                           -- Data Proposal hash
    parent_dp_hash TEXT NOT NULL,                           -- Data Proposal hash
    tx_hash TEXT NOT NULL,
    version INT NOT NULL,
    transaction_type transaction_type NOT NULL,      -- Field to identify the type of transaction (used for joins)
    transaction_status transaction_status NOT NULL,  -- Field to identify the status of the transaction
    block_hash TEXT REFERENCES blocks(hash) ON DELETE CASCADE,
    block_height INT,
    lane_id TEXT,                           -- Lane ID
    index INT,                              -- Index of the transaction within the block
    identity TEXT,                          -- Identity (NULL except for blob transactions)
    PRIMARY KEY (parent_dp_hash, tx_hash),

    CHECK (length(tx_hash) = 64),
    CHECK (length(lane_id) = 96)
);

CREATE INDEX idx_transactions_lane_id ON transactions(lane_id);

CREATE TABLE blobs (
    parent_dp_hash TEXT NOT NULL,  -- Foreign key linking to the parent_dp_hash BlobTransactions
    tx_hash TEXT NOT NULL,  -- Foreign key linking to the tx_hash BlobTransactions
    
    blob_index INT NOT NULL,           -- Index of the blob within the transaction
    identity TEXT NOT NULL,            -- Identity field from the original BlobTransaction struct
    contract_name TEXT NOT NULL,       -- Contract name associated with the blob
    data BYTEA NOT NULL,               -- Actual blob data (stored as binary)
    verified BOOLEAN NOT NULL,         -- Field to indicate if the blob is verified
    PRIMARY KEY (parent_dp_hash, tx_hash, blob_index), -- Composite primary key (parent_dp_hash + tx_hash + blob_index) to uniquely identify each blob
    CHECK (blob_index >= 0),           -- Ensure the index is positive
    FOREIGN KEY (parent_dp_hash, tx_hash) REFERENCES transactions(parent_dp_hash, tx_hash) ON DELETE CASCADE
);
create index idx_blobs_contract_name on blobs(contract_name);

-- This table stores actual proofs, which may not be present in all indexers
CREATE TABLE proofs (
    tx_hash TEXT NOT NULL,    
    parent_dp_hash TEXT NOT NULL,    
    proof BYTEA NOT NULL,
    FOREIGN KEY (parent_dp_hash, tx_hash) REFERENCES transactions(parent_dp_hash, tx_hash) ON DELETE CASCADE,
    PRIMARY KEY (tx_hash, parent_dp_hash)
);

-- This table stores one line for each hyle output in a VerifiedProof
CREATE TABLE blob_proof_outputs (
    blob_parent_dp_hash  TEXT NOT NULL,         -- Foreign key linking to the BlobTransactions    
    blob_tx_hash  TEXT NOT NULL,         -- Foreign key linking to the BlobTransactions    
    proof_parent_dp_hash TEXT NOT NULL,
    proof_tx_hash TEXT NOT NULL,
    blob_index INT NOT NULL,            -- Index of the blob within the transaction
    blob_proof_output_index INT NOT NULL, -- Index of the blob proof output within the proof
    contract_name TEXT NOT NULL,       -- Contract name associated with the blob
    hyle_output JSONB NOT NULL,        -- Additional metadata stored in JSONB format
    settled BOOLEAN NOT NULL,       -- Was this blob proof output used in settlement ? 
    PRIMARY KEY (proof_parent_dp_hash, proof_tx_hash, blob_parent_dp_hash, blob_tx_hash, blob_index, blob_proof_output_index),
    FOREIGN KEY (blob_parent_dp_hash, blob_tx_hash, blob_index) REFERENCES blobs(parent_dp_hash, tx_hash, blob_index) ON DELETE CASCADE,
    FOREIGN KEY (blob_tx_hash, blob_parent_dp_hash) REFERENCES transactions(tx_hash, parent_dp_hash) ON DELETE CASCADE,
    FOREIGN KEY (proof_tx_hash, proof_parent_dp_hash) REFERENCES transactions(tx_hash, parent_dp_hash) ON DELETE CASCADE,
    UNIQUE (blob_parent_dp_hash, blob_tx_hash, blob_index, blob_proof_output_index)
);

CREATE TABLE contracts (
    tx_hash TEXT NOT NULL,
    parent_dp_hash TEXT NOT NULL,
    verifier TEXT NOT NULL,
    program_id BYTEA NOT NULL,
    timeout_window BIGINT,
    state_commitment BYTEA NOT NULL,
    contract_name TEXT PRIMARY KEY NOT NULL,
    FOREIGN KEY (parent_dp_hash, tx_hash) REFERENCES transactions(parent_dp_hash, tx_hash) ON DELETE CASCADE
);

CREATE TABLE contract_state (
    contract_name TEXT NOT NULL,                                          -- Name of the contract
    block_hash TEXT NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE,   -- Block where the state is captured
    state_commitment BYTEA NOT NULL,                                          -- The contract state stored in JSON format for flexibility
    PRIMARY KEY (contract_name, block_hash)
);

CREATE TABLE transaction_state_events (
    block_hash TEXT NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE,
    block_height INT,
    index INT,
    tx_hash TEXT NOT NULL,
    parent_dp_hash TEXT NOT NULL,
    FOREIGN KEY (tx_hash, parent_dp_hash) REFERENCES transactions(tx_hash, parent_dp_hash) ON DELETE CASCADE,
    
    events JSONB NOT NULL
);


CREATE INDEX idx_bpo_on_proof_tx
  ON blob_proof_outputs (proof_tx_hash);

CREATE INDEX idx_proofs_on_tx_hash
  ON proofs (tx_hash);

CREATE INDEX idx_bpo_prooftx_contract
  ON blob_proof_outputs (proof_tx_hash, contract_name);

-- Index for get tx by hash
CREATE INDEX idx_tx_fast_lookup
  ON transactions (
    tx_hash,
    transaction_type,
    block_height   DESC,
    index          DESC
  )
INCLUDE (block_hash);

create table txs_contracts (
    parent_dp_hash TEXT NOT NULL,  -- Foreign key linking to the parent_dp_hash BlobTransactions
    tx_hash TEXT NOT NULL,  -- Foreign key linking to the tx_hash BlobTransactions
    contract_name TEXT NOT NULL,       -- Contract name associated with the blob
    PRIMARY KEY (parent_dp_hash, tx_hash, contract_name)
);
create index idx_txs_contracts_name on txs_contracts(contract_name);

