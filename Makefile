FUZZTIME ?= 30s

.PHONY: test vet ci integration soak fuzz

test:
	go test ./...

vet:
	go vet ./...

# Offline verification ring, the same sequence .github/workflows/ci.yml runs
# (gofmt clean, vet across every build-tag set, race tests incl. golden-frame
# replay, build). No broker required.
ci:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then \
		echo "gofmt needed on:"; echo "$$out"; exit 1; fi
	go vet ./...
	go vet -tags integration ./...
	go vet -tags soak ./...
	go test ./... -race -count=1
	go build ./...

# Live integration ring. Provide brokers yourself and export
# CUBRID_TEST_DSN_<ver> (93/101/102/110/114) for each version you run,
# e.g. CUBRID_TEST_DSN_114=cubrid://dba@host:33000/testdb.
# The full five-version run takes >10m (go test's default -timeout).
# -p 1 serializes the root and cubrid test binaries: both hit the same
# DSNs, and the cubrid package's stale-table sweep (sweepStaleTables)
# reclaims every go_* table, including ones a concurrently running
# sibling binary just created. Suites must share a server back-to-back,
# never simultaneously.
integration:
	go test -tags integration -count=1 -timeout 40m -p 1 ./...

soak:
	go test -tags soak -count=1 -race -timeout 40m ./...

# Native fuzzing allows one -fuzz pattern per run: loop every Fuzz target
# in the protocol package for FUZZTIME each (override: FUZZTIME=100s).
fuzz:
	@set -e; for t in $$(go test ./cubrid/internal/protocol/ -list '^Fuzz' | grep '^Fuzz'); do \
		echo "=== fuzz $$t ($(FUZZTIME))"; \
		go test ./cubrid/internal/protocol/ -run '^$$' -fuzz "^$$t$$" -fuzztime $(FUZZTIME); \
	done
