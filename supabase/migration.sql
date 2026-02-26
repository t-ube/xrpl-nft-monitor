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
CREATE INDEX idx_modify_cursor ON nft_modify_history(ledger_index, tx_hash);

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
CREATE INDEX idx_mint_cursor ON nft_mint_history(ledger_index, tx_hash);


-- NFT売買履歴テーブル（ブローカー取引対応）
CREATE TABLE nft_sale_history (
    tx_hash VARCHAR(64) PRIMARY KEY,
    nftoken_id VARCHAR(64) NOT NULL,
    ledger_index BIGINT NOT NULL,
    tx_date TIMESTAMPTZ NOT NULL,
    issuer VARCHAR(35) NOT NULL,
    taxon INT NOT NULL,
    seller VARCHAR(35) NOT NULL,
    buyer VARCHAR(35) NOT NULL,
    broker VARCHAR(35),                    -- ブローカー（xrp.cafe等）。NULL = 直接取引
    -- 売り手の受取額
    sell_price_drops BIGINT,               -- XRP (in drops)
    sell_price_currency VARCHAR(3),         -- IOU currency code
    sell_price_value VARCHAR(50),           -- IOU amount
    sell_price_issuer VARCHAR(35),          -- IOU issuer
    -- 買い手の支払額（ブローカー取引時のみ売り手と異なる）
    buy_price_drops BIGINT,
    buy_price_currency VARCHAR(3),
    buy_price_value VARCHAR(50),
    buy_price_issuer VARCHAR(35),
    uri TEXT,
    sale_type VARCHAR(20) NOT NULL DEFAULT 'sale',  -- sale / distribution / transfer
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sale_nftoken_id ON nft_sale_history(nftoken_id);
CREATE INDEX idx_sale_issuer ON nft_sale_history(issuer);
CREATE INDEX idx_sale_seller ON nft_sale_history(seller);
CREATE INDEX idx_sale_buyer ON nft_sale_history(buyer);
CREATE INDEX idx_sale_broker ON nft_sale_history(broker);
CREATE INDEX idx_sale_ledger_index ON nft_sale_history(ledger_index DESC);
CREATE INDEX idx_sale_tx_date ON nft_sale_history(tx_date DESC);
CREATE INDEX idx_sale_cursor ON nft_sale_history(ledger_index, tx_hash);
CREATE INDEX idx_sale_type ON nft_sale_history(sale_type);

-- monitor_state に追加
INSERT INTO monitor_state (key, value) VALUES ('last_sale_ledger_index', 99470404);

