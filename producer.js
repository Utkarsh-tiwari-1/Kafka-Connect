require("dotenv").config();
const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

async function init() {
    const producer = kafka.producer();

    console.log("Connecting producer...");
    await producer.connect();
    console.log("Producer connected successfully ");

    rl.setPrompt("> ");
    rl.prompt();

    rl.on("line", async (line) => {
        const [riderName, location] = line.trim().split(" ");

        if (!riderName || !location) {
            console.log(" Format error: Please enter in format '<riderName> <location>'");
            rl.prompt();
            return;
        }

        const loc = location.toLowerCase();
        const partition = loc === "north" ? 0 : loc === "south" ? 1 : null;

        if (partition === null) {
            console.log(` Unknown location '${location}'. Only 'north' or 'south' are valid.`);
            rl.prompt();
            return;
        }

        const message = {
            topic: "rider-updates",
            messages: [
                {
                    partition,
                    key: "location-update",
                    value: JSON.stringify({ name: riderName, location }),
                },
            ],
        };

        try {
            await producer.send(message);
            console.log(` Sent to partition ${partition}: ${JSON.stringify(message.messages[0])}`);
        } catch (err) {
            console.error("Failed to send message:", err);
        }

        rl.prompt();
    });

    rl.on("close", async () => {
        console.log("Disconnecting producer...");
        await producer.disconnect();
        console.log("Producer disconnected. 👋");
        process.exit(0);
    });

    process.on("SIGINT", async () => {
        rl.close();
    });
}

init().catch((err) => {
    console.error(" Producer initialization failed:", err);
    process.exit(1);
});
