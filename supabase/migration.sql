-- 監視状態テーブル
CREATE TABLE monitor_state (
    key VARCHAR(50) PRIMARY KEY,
    value BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 初期値（適宜変更）
INSERT INTO monitor_state (key, value) VALUES ('last_modify_ledger_index', 99470404);
INSERT INTO monitor_state (key, value) VALUES ('last_mint_ledger_index', 99470404);

-- NFTokenModify 履歴テーブル
CREATE TABLE nft_modify_history (
    tx_hash VARCHAR(64) PRIMARY KEY,
    nftoken_id VARCHAR(64) NOT NULL,
    ledger_index BIGINT NOT NULL,
    tx_date TIMESTAMPTZ NOT NULL,
    issuer VARCHAR(35) NOT NULL,
    owner VARCHAR(35) NOT NULL,
    taxon INT NOT NULL,
    sequence INT NOT NULL,
    previous_uri TEXT,
    current_uri TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- インデックス
CREATE INDEX idx_modify_nftoken_id ON nft_modify_history(nftoken_id);
CREATE INDEX idx_modify_issuer ON nft_modify_history(issuer);
CREATE INDEX idx_modify_owner ON nft_modify_history(owner);
CREATE INDEX idx_modify_taxon ON nft_modify_history(issuer, taxon);
CREATE INDEX idx_modify_ledger_index ON nft_modify_history(ledger_index DESC);
CREATE INDEX idx_modify_tx_date ON nft_modify_history(tx_date DESC);


-- NFTokenMint 履歴テーブル
CREATE TABLE nft_mint_history (
    tx_hash VARCHAR(64) PRIMARY KEY,
    nftoken_id VARCHAR(64) NOT NULL,
    ledger_index BIGINT NOT NULL,
    tx_date TIMESTAMPTZ NOT NULL,
    issuer VARCHAR(35) NOT NULL,
    owner VARCHAR(35) NOT NULL,
    taxon INT NOT NULL,
    sequence INT NOT NULL,
    uri TEXT,
    flags INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- インデックス
CREATE INDEX idx_mint_nftoken_id ON nft_mint_history(nftoken_id);
CREATE INDEX idx_mint_issuer ON nft_mint_history(issuer);
CREATE INDEX idx_mint_owner ON nft_mint_history(owner);
CREATE INDEX idx_mint_ledger_index ON nft_mint_history(ledger_index DESC);
CREATE INDEX idx_mint_tx_date ON nft_mint_history(tx_date DESC);
