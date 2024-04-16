const { create: createStatusController } = require("../../lib/status-controller.js");
const { performance } = require("perf_hooks");

const findErrorRootCause = ({ error }) => {
    if (error.cause) {
        return findErrorRootCause({ error: error.cause });
    }

    return error;
};

module.exports = function (RED) {
    function BalluffIoLinkAcyclicOutput(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        let connecting = false;
        let lastConnection = undefined;
        let ioLinkHandle = undefined;
        let lastError = undefined;
        let closed = false;

        let sendQueue = [];
        let debounceTimeoutHandle = undefined;

        let writePending = false;
        let lastWriteAt = undefined;
        let updateStatusIconTimeoutHandle = undefined;
        let notConnectedMessageShown = false;

        const busyLedMinDurationMs = 300;

        const statusController = createStatusController({
            node,
            busyText: "writing",
            connectedText: "connected"
        });

        const updateStatusIcon = () => {
            if (connecting) {
                statusController.connecting();
                return;
            }

            if (lastConnection === undefined) {
                statusController.disconnected();
                return;
            }

            if (lastError !== undefined) {
                statusController.error();
                return;
            }

            if (writePending) {
                statusController.busy();
                return;
            }

            const now = performance.now();
            const timeSinceLastWrite = lastWriteAt === undefined ? undefined : now - lastWriteAt;
            const remainingTriggerStatusTime = timeSinceLastWrite === undefined ? 0 : busyLedMinDurationMs - timeSinceLastWrite;

            if (remainingTriggerStatusTime > 0) {
                statusController.busy();

                updateStatusIconTimeoutHandle = setTimeout(() => {
                    updateStatusIcon();
                }, remainingTriggerStatusTime + 5);

                return;
            }

            statusController.connected();
        };

        updateStatusIcon();

        const debounceTimeMs = parseInt(config.debounce);
        if (isNaN(debounceTimeMs)) {
            node.error("invalid debounce time");
            return;
        }

        const connectionConfigNode = RED.nodes.getNode(config.connection);
        if (connectionConfigNode === undefined) {
            node.error("missing connection");
            return;
        }

        const registration = connectionConfigNode.registerBalluffNode({

            onStateChange: ({ connecting: newConnecting, connection, ioLinkHandle: newIoLinkHandle, error }) => {

                if (closed) {
                    node.warn("state change event after node close");
                    return;
                }

                const changed = connection !== lastConnection;
                lastConnection = connection;
                connecting = newConnecting;

                if (changed) {
                    lastError = undefined;
                    lastWriteAt = undefined;

                    ioLinkHandle = newIoLinkHandle;
                }

                updateStatusIcon();
            },
        });

        const maybeWriteNextRequest = () => {
            if (closed) {
                return;
            }
            
            if (writePending) {
                return;
            }

            if (ioLinkHandle === undefined) {
                return;
            }

            if (sendQueue.length === 0) {
                return;
            }

            if (debounceTimeoutHandle !== undefined) {
                return;
            }

            writePending = true;
            lastWriteAt = performance.now();

            lastError = undefined;

            const nextRequest = sendQueue[0];
            sendQueue = sendQueue.slice(1);

            // make sure synchronous errors are also caught
            Promise.resolve().then(() => {
                return ioLinkHandle.writeAcyclicProcessData({
                    index: nextRequest.index,
                    subIndex: nextRequest.subIndex,
                    data: nextRequest.payload
                });
            }).then(({ error }) => {
                return {
                    error
                };
            }, (err) => {
                return {
                    error: err
                };
            }).then(({ error }) => {

                if (closed) {
                    return;
                }

                writePending = false;

                if (error !== undefined) {
                    lastError = error;

                    // log error to console with full cause chain
                    console.error(`writing of acyclic io link data failed`, error);

                    // as Node RED does not show the full cause chain
                    // we only show the root cause
                    const rootError = findErrorRootCause({ error });
                    node.error(rootError, rootError);
                }

                debounceTimeoutHandle = setTimeout(() => {
                    debounceTimeoutHandle = undefined;
                    maybeWriteNextRequest();
                }, debounceTimeMs);

                updateStatusIcon();
            });

            nextProcessData = undefined;
            updateStatusIcon();
        };

        const queueRequest = ({ inputMessage }) => {
            let queued = false;

            // first, try to find already queued requests and update them
            sendQueue = sendQueue.map((req) => {
                if (req.index == inputMessage.index && req.subIndex === inputMessage.subIndex) {
                    queued = true;
                    return inputMessage;
                } else {
                    return req;
                }
            });

            // if no request of index / subIndex tuple was found, add the new request
            if (!queued) {
                sendQueue = [
                    ...sendQueue,
                    inputMessage
                ];
            }
        };

        const validateRequest = ({ inputMessage }) => {
            if (isNaN(inputMessage.index)) {
                throw Error("index must be a number");
            }

            if (isNaN(inputMessage.subIndex)) {
                throw Error("subIndex must be a number");
            }

            if (inputMessage.payload === undefined) {
                throw Error("payload must be defined");
            }
        };

        const checkIfConnected = () => {
            if (ioLinkHandle === undefined) {

                if (!notConnectedMessageShown) {
                    node.error("not connected, further errors will be silenced until connected again");
                    notConnectedMessageShown = true;
                }

                return;
            }

            notConnectedMessageShown = false;
        };

        node.on("input", (inputMessage) => {
            validateRequest({ inputMessage });
            checkIfConnected();            
            queueRequest({ inputMessage });
            maybeWriteNextRequest();
        });

        node.on("close", () => {
            closed = true;
            registration.close();
        });
    }

    RED.nodes.registerType("balluff-iolink-acyclic-output", BalluffIoLinkAcyclicOutput);
}
