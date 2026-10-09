const express = require('express');
const { connect, StringCodec } = require('nats');
const Ajv = require('ajv');

const app = express();
app.use(express.json());

const NATS_URL = process.env.NATS_URL || 'nats://nats-service:4222';
const PORT = process.env.PORT || 8080;
const sc = StringCodec();

let nc;
let js;

// JSON Schema for event validation
const eventSchema = {
    type: "object",
    properties: {
        event_name: { type: "string", minLength: 1 },
        correlation_key: { type: "string", minLength: 1 },
        element_id: { type: "string" },
        output_data: {
            type: "object",
            properties: {
                bpmn_xml: { type: "string" }
            }
        }
    },
    required: ["event_name", "correlation_key"],
    additionalProperties: true
};

const ajv = new Ajv({ allErrors: true });
const validateEvent = ajv.compile(eventSchema);

async function initNats() {
    try {
        nc = await connect({ servers: NATS_URL });
        js = nc.jetstream();
        console.log(`Connected to NATS at ${NATS_URL}`);
    } catch (err) {
        console.error('Error connecting to NATS:', err);
        process.exit(1);
    }
}

app.post('/api/v1/process/instances/:id/events', async (req, res) => {
    const instanceId = req.params.id;
    console.log(`📩 [Gateway] Incoming HTTP request for instance_id: ${instanceId}`);

    const isValid = validateEvent(req.body);

    if (!isValid) {
        console.warn(`⚠️ [Gateway] Schema validation failed for ${instanceId}`);
        return res.status(400).json({
            error: "Validation Failed: Request body does not match expected JSON schema",
            details: validateEvent.errors.map(err => ({
                field: err.instancePath || 'root',
                message: err.message,
                params: err.params
            })),
            expected_schema: eventSchema
        });
    }

    const { event_name, correlation_key, element_id, output_data } = req.body;

    const natsPayload = JSON.stringify({
        event_id: correlation_key,
        instance_id: instanceId,
        element_id: element_id || '',
        payload: output_data || {}
    });

    const subject = `mes.in.v1.event.erp.${event_name}`;

    try {
        await js.publish(subject, sc.encode(natsPayload));
        console.log(`✅ [Gateway] Event published to NATS on subject: ${subject}`);
        res.status(202).json({ 
            status: 'event_accepted', 
            instance_id: instanceId,
            subject 
        });
    } catch (err) {
        console.error('❌ [Gateway] Failed to publish event to NATS:', err);
        res.status(500).json({ error: 'Failed to publish event to NATS' });
    }
});

initNats().then(() => {
    app.listen(PORT, () => {
        console.log(`HTTP Cluster Gateway listening on port ${PORT}`);
    });
});

process.on('SIGINT', async () => {
    if (nc) await nc.close();
    process.exit(0);
});