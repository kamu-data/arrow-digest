TEST_LOG_PARAMS=RUST_LOG_SPAN_EVENTS=new,close RUST_LOG=info

###############################################################################
# Lint
###############################################################################

.PHONY: lint
lint:
	cargo fmt --check
	cargo deny check
	cargo clippy --workspace --all-targets -- -D warnings


###############################################################################
# Test
###############################################################################

.PHONY: test
test:
	$(TEST_LOG_PARAMS) cargo test
