#include "s3_limits.hpp"
#include <iostream>
#include <limits>

int main() {
    using namespace grv_s3_limits;
    std::size_t cost = 0;
    auto require = [](bool condition, const char *message) {
        if (!condition) { std::cerr << message << '\n'; return false; }
        return true;
    };
    if (!require(Admit(0, 0, MAX_URI_BYTES, MAX_VALIDATOR_BYTES, cost), "bounded maximum file was refused") ||
        !require(cost >= 24 * MAX_URI_BYTES + MAX_VERSION_BYTES + MAX_VALIDATOR_BYTES, "retained string and version capacity was undercounted")) { return 1; }
    if (!require(Admit(METADATA_BYTES-cost, 1, MAX_URI_BYTES, MAX_VALIDATOR_BYTES, cost), "exact budget boundary was refused") ||
        !require(!Admit(METADATA_BYTES-cost+1, 1, MAX_URI_BYTES, MAX_VALIDATOR_BYTES, cost), "one-byte overflow was adopted")) { return 1; }
    if (!require(!Admit(std::numeric_limits<std::size_t>::max(), 0, 1, 1, cost), "ledger overflow wrapped") ||
        !require(!Admit(0, MAX_OBJECTS, 1, 1, cost), "object count overflow was adopted") ||
        !require(!Admit(0, 0, MAX_URI_BYTES+1, 1, cost), "oversized URI was adopted") ||
        !require(!Admit(0, 0, 1, MAX_VALIDATOR_BYTES+1, cost), "oversized validator was adopted") ||
        !require(!Admit(0, 0, 1, 0, cost), "missing validator was adopted")) { return 1; }
    std::size_t used = 0, objects = 0;
    while (Admit(used, objects, 350, 34, cost)) { used += cost; ++objects; }
    if (!require(objects >= 3 && objects < 256 && used <= METADATA_BYTES, "many-file admission escaped its independent limit") ||
        !require(METADATA_BYTES-used < cost, "many-file refusal occurred before the byte limit")) { return 1; }
    std::cout << "PASS exact S3 metadata admission, retained capacity and overflow boundaries\n";
    return 0;
}
