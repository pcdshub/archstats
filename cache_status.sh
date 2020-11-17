#!/bin/bash

ARCH_HOST=${ARCH_HOST=localhost:17665}
APPLIANCES=${APPLIANCES=pscaa01 pscaa02}

echo "Caching information for testing from ${ARCH_HOST}..."
# curl ${ARCH_HOST}/mgmt/bpl/getApplianceMetrics  > archstats/tests/json/getApplianceMetrics.json
# curl ${ARCH_HOST}/mgmt/bpl/getStorageMetrics  > archstats/tests/json/getStorageMetrics.json
# curl ${ARCH_HOST}/mgmt/bpl/getInstanceMetrics  > archstats/tests/json/getInstanceMetrics.json

# for appliance in ${APPLIANCES}; do
#     echo "Appliance ${appliance}"
#     curl "${ARCH_HOST}/mgmt/bpl/getApplianceMetricsForAppliance?appliance=${appliance}"  > archstats/tests/json/getApplianceMetricsForAppliance-${appliance}.json
# done
