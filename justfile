# Copyright 2018-2020 Cargill Incorporated
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set dotenv-load := true

crates := '\
    libsawtooth \
    examples/address_generator \
    examples/sabre_command \
    examples/sabre_smallbank \
    examples/simple_xo \
    '

crates_wasm := '\
    examples/sabre_command \
    examples/sabre_smallbank \
    '

features := '\
    --features=experimental \
    --features=stable \
    --features=default \
    --no-default-features \
    '

check:
    #!/usr/bin/env sh
    set -e
    for feature in $(echo {{features}})
    do
        for crate in $(echo {{crates}})
        do
            cmd="cargo check --tests --manifest-path=$crate/Cargo.toml $feature"
            echo "\033[1m$cmd\033[0m"
            $cmd
        done
    done
    echo "\n\033[92mBuild Success\033[0m\n"

build:
    #!/usr/bin/env sh
    set -e
    for feature in $(echo {{features}})
    do
        for crate in $(echo {{crates}})
        do
            cmd="cargo build --tests --manifest-path=$crate/Cargo.toml $feature"
            echo "\033[1m$cmd\033[0m"
            $cmd
        done
    done
    for feature in $(echo {{features}})
    do
        for crate in $(echo {{crates_wasm}})
        do
            cmd="cargo build --tests --target wasm32-unknown-unknown --manifest-path=$crate/Cargo.toml $BUILD_MODE $feature"
            echo "\033[1m$cmd\033[0m"
            RUSTFLAGS="-D warnings" $cmd
            $cmd
        done
    done
    echo "\n\033[92mBuild Success\033[0m\n"

clean:
    #!/usr/bin/env sh
    set -e
    for crate in $(echo {{crates}})
    do
        cmd="cargo clean --manifest-path=$crate/Cargo.toml"
        echo "\033[1m$cmd\033[0m"
        $cmd
        cmd="rm -f $crate/Cargo.lock"
        echo "\033[1m$cmd\033[0m"
        $cmd
    done

fix:
    #!/usr/bin/env sh
    set -e
    for crate in $(echo {{crates}})
    do
        for feature in $(echo {{features}})
        do
            cmd="cargo fix --manifest-path=$crate/Cargo.toml $feature"
            echo "\033[1m$cmd\033[0m"
            $cmd
        done
    done
    echo "\n\033[92mFix Success\033[0m\n"

lint:
    #!/usr/bin/env sh
    set -e
    for crate in $(echo {{crates}})
    do
        cmd="cargo fmt --manifest-path=$crate/Cargo.toml -- --check"
        echo "\033[1m$cmd\033[0m"
        $cmd
    done
    for crate in $(echo {{crates}})
    do
        for feature in $(echo {{features}})
        do
            cmd="cargo clippy --manifest-path=$crate/Cargo.toml $feature -- -D warnings"
            echo "\033[1m$cmd\033[0m"
            $cmd
        done
    done
    echo "\n\033[92mLint Success\033[0m\n"


docker-lint:
    docker-compose -f docker/compose/run-lint.yaml up \
        --build \
        --abort-on-container-exit \
        --exit-code-from \
        lint-libsawtooth

test:
    #!/usr/bin/env sh
    set -e
    for feature in $(echo {{features}})
    do
        for crate in $(echo {{crates}})
        do
            cmd="cargo build --tests --manifest-path=$crate/Cargo.toml $feature"
            echo "\033[1m$cmd\033[0m"
            $cmd
            cmd="cd $crate && cargo test $feature"
            echo "\033[1m$cmd\033[0m"
            (eval $cmd)
        done
    done
    echo "\n\033[92mTest Success\033[0m\n"

docker-test:
    #!/usr/bin/env sh
    set -e

    trap "docker-compose -f docker/compose/run-tests.yaml down" EXIT

    docker-compose -f docker/compose/run-tests.yaml build
    docker-compose -f docker/compose/run-tests.yaml run --rm test-libsawtooth \
      /bin/bash -c "just test" --abort-on-container-exit test-libsawtooth

    docker-compose -f docker/compose/run-tests.yaml up --detach postgres-db

    docker-compose -f docker/compose/run-tests.yaml run --rm test-libsawtooth \
       /bin/bash -c \
       "cargo test --manifest-path /project/libsawtooth/libsawtooth/Cargo.toml \
          --features stable,transact-sawtooth-compat -- transact::sawtooth && \
        cargo test --manifest-path /project/libsawtooth/libsawtooth/Cargo.toml \
          --features stable,transact-state-merkle-sql-postgres-tests && \
        (cd examples/sabre_smallbank && cargo test)"
