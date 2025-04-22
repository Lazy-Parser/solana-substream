use substreams_solana::pb::sf::solana::r#type::v1::{Block, CompiledInstruction};
use mydata::v1::PriceUpdate;
use anyhow::{anyhow, bail};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bs58;

mod mydata {
    pub mod v1 {
        include!("./pb/mydata.v1.rs");
    }
}

// MINT адреса
const TRUMP_MINT: &str = "47Y6gNzZ2FZgdkw9VGuPHRikQojrTn9zoPqsMsQwPnnR";
const USDT_MINT: &str = "Es9vMFrzaCERxVbDSb8wMNiNnp8xpEo1Y8DQ2ozf3xvv";
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

// Парсим volume из данных transfer инструкции
fn extract_volume(instr: &CompiledInstruction) -> Option<f64> {
    let data = instr.data.clone();
    if data.first() == Some(&3u8) && data.len() >= 9 {
        let raw = &data[1..9];
        let amount = u64::from_le_bytes(raw.try_into().ok()?);
        Some(amount as f64 / 1_000_000.0) // предположим 6 знаков
    } else {
        None
    }
}

#[substreams::handlers::map]
fn map_my_data(block: Block) -> Result<PriceUpdate, anyhow::Error> {
    for tx in &block.transactions {
        let Some(inner) = &tx.transaction else { continue };
        let Some(msg) = &inner.message else { continue };

        let mut trump_amount = 0.0;
        let mut usdt_amount = 0.0;

        for instr in &msg.instructions {
            let program_id_idx = instr.program_id_index as usize;
            let program_pubkey = msg.account_keys.get(program_id_idx)
                .ok_or_else(|| anyhow!("Invalid program ID index"))?;

            let program_pubkey_str = bs58::encode(program_pubkey).into_string();
            if program_pubkey_str != TOKEN_PROGRAM_ID {
                continue;
            }

            let involved_accounts: Vec<String> = instr
                .accounts
                .iter()
                .filter_map(|i| msg.account_keys.get(*i as usize))
                .map(|key| bs58::encode(key).into_string())
                .collect();

            if involved_accounts.iter().any(|a| a == TRUMP_MINT) {
                if let Some(amount) = extract_volume(instr) {
                    trump_amount += amount;
                }
            }

            if involved_accounts.iter().any(|a| a == USDT_MINT) {
                if let Some(amount) = extract_volume(instr) {
                    usdt_amount += amount;
                }
            }
        }

        if trump_amount > 0.0 && usdt_amount > 0.0 {
            let price = usdt_amount / trump_amount;

            let update = PriceUpdate {
                pair_address: "TRUMP/USDT".to_string(),
                token0: "TRUMP".to_string(),
                token1: "USDT".to_string(),
                price_usd: price,
                volume_usd: usdt_amount,
                block_number: block.slot,
                timestamp: block.block_time.map(|t| t.timestamp.max(0) as u64).unwrap_or(0),
            };

            println!("{} price", price );

            return Ok(update);
        }
    }

    bail!("No TRUMP/USDT transfer found")
}
