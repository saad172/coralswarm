//! HTTP client for `POST https://api.typesafe.ai/v1/systemone`.
//!
//! Opt-in only since 2026-10-08 (`JEV_PROVIDER=typesafe` + `JEV_API_KEY`);
//! the default Jev backend is [`super::openrouter`]. Not metered into
//! `llm_usage` — TypeSafe bills by its own credits.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;

use super::{
    jev_timeout_secs, DecisionError, DecisionProvider, DecisionRequest, DecisionResponse,
};

const DEFAULT_BASE_URL: &str = "https://api.typesafe.ai";
const EVALUATE_PATH: &str = "/v1/systemone";
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_BASE_MS: u64 = 250;
const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 2_000;

pub struct TypeSafeClient {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl TypeSafeClient {
    pub fn from_api_key(api_key: &str) -> Result<Self, DecisionError> {
        let timeout = Duration::from_secs(jev_timeout_secs());
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| DecisionError::Upstream {
                status: 0,
                body: e.to_string(),
            })?;
        let base_url = std::env::var("JEV_BASE_URL")
            .ok()
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
        Ok(Self {
            http,
            api_key: api_key.to_string(),
            base_url,
        })
    }

    #[cfg(test)]
    pub fn with_http(http: reqwest::Client, api_key: String, base_url: String) -> Self {
        Self {
            http,
            api_key,
            base_url,
        }
    }

    fn url(&self) -> String {
        format!("{}{EVALUATE_PATH}", self.base_url)
    }
}

#[async_trait]
impl DecisionProvider for TypeSafeClient {
    async fn evaluate(&self, req: DecisionRequest) -> Result<DecisionResponse, DecisionError> {
        if req.questions.is_empty() {
            return Ok(DecisionResponse {
                model: req.model,
                answers: Default::default(),
                usage: None,
            });
        }

        let max_attempts = std::env::var("JEV_MAX_RETRIES")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .map(|n| n.max(1))
            .unwrap_or(DEFAULT_MAX_ATTEMPTS);
        let base_ms = std::env::var("JEV_RETRY_BASE_MS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(DEFAULT_RETRY_BASE_MS);
        let max_delay_ms = std::env::var("JEV_RETRY_MAX_DELAY_MS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(DEFAULT_RETRY_MAX_DELAY_MS);

        // Bound the whole retry loop to one request budget. Per-attempt
        // `reqwest` timeout + backoff would otherwise accumulate past
        // `JEV_TIMEOUT_SECS` and delay fail-open.
        let deadline = std::time::Instant::now() + Duration::from_secs(jev_timeout_secs());
        let mut last_err = DecisionError::Timeout;
        for attempt in 0..max_attempts {
            if std::time::Instant::now() >= deadline {
                return Err(last_err);
            }
            match self.once(&req).await {
                Ok(resp) => return Ok(resp),
                Err(e) if retryable(&e) && attempt + 1 < max_attempts => {
                    let exp = base_ms.saturating_mul(1u64 << attempt.min(8));
                    let sleep_ms = exp.min(max_delay_ms);
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero()
                        || Duration::from_millis(sleep_ms) >= remaining
                    {
                        return Err(e);
                    }
                    tracing::warn!(
                        attempt,
                        error = %e,
                        sleep_ms,
                        "TypeSafe evaluate retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                    last_err = e;
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err)
    }
}

fn retryable(err: &DecisionError) -> bool {
    match err {
        DecisionError::Timeout | DecisionError::RateLimit(_) => true,
        DecisionError::Upstream { status, .. } => *status == 0 || *status >= 500,
        _ => false,
    }
}

impl TypeSafeClient {
    async fn once(&self, req: &DecisionRequest) -> Result<DecisionResponse, DecisionError> {
        let resp = self
            .http
            .post(self.url())
            .bearer_auth(&self.api_key)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(req)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() || e.is_connect() {
                    DecisionError::Timeout
                } else {
                    DecisionError::Upstream {
                        status: 0,
                        body: e.to_string(),
                    }
                }
            })?;

        let status = resp.status();
        let body = resp.text().await.map_err(|e| {
            if e.is_timeout() {
                DecisionError::Timeout
            } else {
                DecisionError::Upstream {
                    status: 0,
                    body: e.to_string(),
                }
            }
        })?;
        if status == StatusCode::UNAUTHORIZED {
            return Err(DecisionError::Auth(body));
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(DecisionError::RateLimit(body));
        }
        if status.as_u16() == 529 {
            return Err(DecisionError::Upstream {
                status: 529,
                body,
            });
        }
        if !status.is_success() {
            return Err(DecisionError::Upstream {
                status: status.as_u16(),
                body,
            });
        }
        serde_json::from_str(&body).map_err(|e| DecisionError::Parse(format!("{e}: {body}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::{Answer, Question};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn request_serializes_systemone_shape() {
        let mut questions = BTreeMap::new();
        questions.insert(
            "is_urgent".into(),
            Question::Noul {
                instructions: json!("Does this convey urgency?"),
                criteria: None,
            },
        );
        questions.insert(
            "dept".into(),
            Question::Choice {
                instructions: json!("Which team?"),
                criteria: BTreeMap::from([
                    ("billing".into(), Some("payments".into())),
                    ("technical".into(), None),
                ]),
            },
        );
        questions.insert(
            "rank".into(),
            Question::Score {
                instructions: json!("How useful?"),
                criteria: vec!["drop".into(), "keep".into()],
            },
        );
        let req = DecisionRequest {
            state: json!({"ask": "hello"}),
            model: "jev-latest".into(),
            questions,
            op: crate::decision::JevOp::Ask,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["model"], "jev-latest");
        assert!(v.get("op").is_none(), "op is internal, never on the System One wire");
        assert_eq!(v["questions"]["is_urgent"]["type"], "noul");
        assert_eq!(v["questions"]["dept"]["type"], "choice");
        assert_eq!(v["questions"]["rank"]["type"], "score");
        assert_eq!(v["questions"]["rank"]["criteria"][0], "drop");
    }

    #[test]
    fn response_parses_all_three_answers() {
        let raw = json!({
            "model": "jev-latest",
            "answers": {
                "is_urgent": { "type": "noul", "noul": 0.92 },
                "dept": {
                    "type": "choice",
                    "choice": "technical",
                    "probabilities": { "billing": 0.1, "technical": 0.9 },
                    "confidence": 0.8
                },
                "rank": {
                    "type": "score",
                    "score": 1.6,
                    "legend": { "0": "drop", "1": "keep" },
                    "probabilities": { "0": 0.2, "1": 0.8 },
                    "confidence": 0.7
                }
            },
            "usage": { "input_tokens": 12, "output_tokens": 4 }
        });
        let parsed: DecisionResponse = serde_json::from_value(raw).unwrap();
        assert_eq!(parsed.model, "jev-latest");
        assert!((parsed.answers["is_urgent"].as_noul().unwrap() - 0.92).abs() < 1e-9);
        assert_eq!(parsed.answers["dept"].as_choice(), Some("technical"));
        assert!((parsed.answers["rank"].as_score().unwrap() - 1.6).abs() < 1e-9);
        match &parsed.answers["rank"] {
            Answer::Score { confidence, .. } => assert!((confidence - 0.7).abs() < 1e-9),
            _ => panic!("expected score"),
        }
    }

    #[test]
    fn timeout_and_5xx_are_retryable() {
        assert!(retryable(&DecisionError::Timeout));
        assert!(retryable(&DecisionError::RateLimit("429".into())));
        assert!(retryable(&DecisionError::Upstream {
            status: 529,
            body: "overloaded".into()
        }));
        assert!(!retryable(&DecisionError::Auth("no".into())));
        assert!(!retryable(&DecisionError::Parse("bad".into())));
    }

    #[test]
    fn choice_confidence_from_probabilities_when_omitted() {
        let raw = json!({
            "model": "jev-latest",
            "answers": {
                "verdict": {
                    "type": "choice",
                    "choice": "conflicts",
                    "probabilities": { "conflicts": 0.91, "different": 0.09 }
                }
            }
        });
        let parsed: DecisionResponse = serde_json::from_value(raw).unwrap();
        assert!((parsed.answers["verdict"].choice_confidence() - 0.91).abs() < 1e-9);
    }

    #[test]
    fn choice_only_response_has_zero_confidence() {
        let raw = json!({
            "model": "jev-latest",
            "answers": {
                "verdict": {
                    "type": "choice",
                    "choice": "same"
                }
            }
        });
        let parsed: DecisionResponse = serde_json::from_value(raw).unwrap();
        assert_eq!(parsed.answers["verdict"].choice_confidence(), 0.0);
    }
}
