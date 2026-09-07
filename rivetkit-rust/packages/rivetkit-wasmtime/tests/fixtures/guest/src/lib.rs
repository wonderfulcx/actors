wit_bindgen::generate!({
    path: "../../../wit",
    world: "cell",
    generate_all,
});

use std::cell::RefCell;

use exports::wonderful::cells::guest::{CommandOutput, Effect, Guest, GuestError};

thread_local! {
    static STATE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

struct TestCell;

impl Guest for TestCell {
    fn activate(
        _bundle: Vec<u8>,
        _manifest: Vec<u8>,
        initial_state: Vec<u8>,
        _input: Option<Vec<u8>>,
    ) -> Result<(), GuestError> {
        STATE.with_borrow_mut(|state| *state = initial_state);
        Ok(())
    }

    fn invoke_command(name: String, input: Vec<u8>) -> Result<CommandOutput, GuestError> {
        if name == "spin" {
            loop {
                STATE.with_borrow_mut(|state| {
                    state.push(1);
                    state.pop();
                });
            }
        }
        if name == "fail" {
            STATE.with_borrow_mut(|state| state.extend_from_slice(&input));
            return Err(GuestError {
                code: "TEST_FAILURE".to_owned(),
                message: "requested failure".to_owned(),
            });
        }
        if name == "sql" {
            let result = wonderful::cells::host::execute_sql(
                "INSERT INTO test_values (value) VALUES (?)",
                &[wonderful::cells::host::SqlValue::Blob(input.clone())],
            )
            .map_err(|message| GuestError {
                code: "TEST_SQL_FAILURE".to_owned(),
                message,
            })?;
            if result.changes != 1 {
                return Err(GuestError {
                    code: "TEST_SQL_CHANGES".to_owned(),
                    message: "unexpected SQL change count".to_owned(),
                });
            }
        }
        let state = STATE.with_borrow_mut(|state| {
            state.extend_from_slice(&input);
            state.clone()
        });
        Ok(CommandOutput {
            state,
            value: input.clone(),
            effects: vec![Effect {
                kind: "test".to_owned(),
                payload: input,
            }],
            destroyed: false,
        })
    }

    fn snapshot() -> Vec<u8> {
        STATE.with_borrow(Clone::clone)
    }
}

export!(TestCell);
