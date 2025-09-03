const { kafka } = require("./client");

// Get groupId from CLI
const group = process.argv[2];

if (!group || group.trim() === "") {
    console.error("Error: Consumer groupId must be provided.");
    console.error("Usage: node consumer.js <groupId>");
    process.exit(1);
}

async function init() {
    const consumer = kafka.consumer({ groupId: group });

    try {
        console.log(` Connecting consumer with groupId: '${group}'...`);
        await consumer.connect();
        console.log(" Connected");

        await consumer.subscribe({
            topic: "rider-updates",
            fromBeginning: true,
        });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value?.toString() || "";
                const key = message.key?.toString() || "null";
                console.log(`[${group}] Topic: ${topic} | Partition: ${partition} | Key: ${key} | Value: ${value}`);
            },
        });

        process.on("SIGINT", async () => {
            console.log(" Disconnecting...");
            await consumer.disconnect();
            console.log(" Disconnected");
            process.exit(0);
        });

    } catch (err) {
        console.error(" Error:", err);
        process.exit(1);
    }
}

init();
