#!/usr/bin/env bash
# Callers may clean only targets allocated by this invocation. These helpers
# never recursively remove leftovers or follow a target symlink.

chirps_target_check() {
    if [[ $# -ne 1 || "$1" != /* || "$1" == / || "$1" == */ || "$1" == */. || "$1" == */.. ]]; then
        printf '%s\n' 'target: an absolute directory path is required' >&2
        return 1
    fi
    if [[ -L "$1" || ( -e "$1" && ! -d "$1" ) ]]; then
        printf '%s\n' 'target: refusing a symlink or non-directory' >&2
        return 1
    fi
}

chirps_target_claim() {
    chirps_target_check "$@" || return 1
    if ! mkdir -- "$1"; then
        printf '%s\n' 'target: allocation requires an absent directory' >&2
        return 1
    fi
}

chirps_target_remove_empty() {
    chirps_target_check "$@" || return 1
    [[ -d "$1" ]] || return 0
    if ! rmdir -- "$1"; then
        printf '%s\n' 'target: refusing to remove a non-empty directory' >&2
        return 1
    fi
}

chirps_target_clean() {
    local target="${1:-}"
    shift || return 1
    chirps_target_check "$target" || return 1
    [[ $# -gt 0 ]] || return 1
    # Cargo can recreate metadata for an absent target. Do not invoke it twice.
    if [[ -d "$target" ]]; then
        "$@" || return 1
    fi
    chirps_target_remove_empty "$target"
}
