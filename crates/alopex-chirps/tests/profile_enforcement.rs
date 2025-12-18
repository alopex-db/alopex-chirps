use alopex_chirps::profile::{MessageProfile, enforce_profile};
use alopex_chirps_wire::frame::{Frame, UserMessage};

fn user_frame() -> Frame {
    Frame::User(UserMessage {
        payload: b"hello".to_vec(),
    })
}

#[test]
fn control_pass_through_for_user_frame() {
    let frame = user_frame();
    let eff = enforce_profile(&frame, MessageProfile::Control).unwrap();
    assert_eq!(eff, MessageProfile::Control);
}

#[test]
fn ephemeral_pass_through_when_not_raft() {
    let frame = user_frame();
    let eff = enforce_profile(&frame, MessageProfile::Ephemeral).unwrap();
    assert_eq!(eff, MessageProfile::Ephemeral);
}

#[test]
fn durable_is_not_implemented() {
    let frame = user_frame();
    let res = enforce_profile(&frame, MessageProfile::Durable);
    assert!(res.is_err(), "Durable should return NotImplemented error");
}

#[ignore = "Raft frame types land in v0.5; enable when Raft Frame variants are available"]
#[test]
fn raft_frames_should_force_control_and_warn() {
    // TODO: replace with real Raft frame variant once alopex_chirps_wire adds AppendEntries/InstallSnapshot.
    let frame = user_frame();
    let eff = enforce_profile(&frame, MessageProfile::Ephemeral).unwrap();
    assert_eq!(eff, MessageProfile::Control);
}
