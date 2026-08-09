use super::{GroupId, MultiRaftError};

const NAMESPACE_PREFIX: &str = "groups/";
const GROUP_ID_HEX_LEN: usize = 16;

/// Encodes a group ID as the only storage namespace accepted by Multi-Raft.
pub fn group_namespace(group_id: GroupId) -> String {
    format!("{NAMESPACE_PREFIX}{:016x}", group_id.0)
}

/// Parses the canonical lowercase hexadecimal group namespace.
///
/// Arbitrary path fragments are deliberately rejected; callers never pass
/// user-controlled path text to a storage factory.
pub fn parse_group_namespace(value: &str) -> Result<GroupId, MultiRaftError> {
    let Some(hex) = value.strip_prefix(NAMESPACE_PREFIX) else {
        return Err(invalid(value));
    };
    if hex.len() != GROUP_ID_HEX_LEN
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(value));
    }
    let raw = u64::from_str_radix(hex, 16).map_err(|_| invalid(value))?;
    let group_id = GroupId(raw);
    if group_namespace(group_id) != value {
        return Err(invalid(value));
    }
    Ok(group_id)
}

fn invalid(value: &str) -> MultiRaftError {
    MultiRaftError::InvalidGroupId {
        value: value.to_owned(),
    }
}
