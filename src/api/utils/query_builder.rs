use anyhow::Result;
use micromap::Map;
use smallvec::SmallVec;
use urlencoding::encode;

pub struct QueryBuilder;

impl QueryBuilder {
    pub fn build(query_params: Map<&str, &str, 8>) -> Result<String> {
        if query_params.is_empty() {
            return Ok(String::new());
        }

        let mut params: SmallVec<[(&str, &str); 8]> = query_params.into_iter().collect();
        params.sort_by(|a, b| a.0.cmp(b.0));

        let capacity = params
            .iter()
            .map(|(k, v)| k.len() + v.len() + 1)
            .sum::<usize>()
            + params.len()
            - 1;

        let mut result = String::with_capacity(capacity);
        for (i, (k, v)) in params.iter().enumerate() {
            if i > 0 {
                result.push('&');
            }
            result.push_str(&encode(k));
            result.push('=');
            result.push_str(&encode(v));
        }
        Ok(result)
    }
}
