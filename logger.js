const { createLogger, format, transports } = require("winston");
const { combine, timestamp, label, printf } = format;

const myFormat = printf(info => {
    return `${info.timestamp} [${info.label}] ${info.level}: ${info.message}`;
});

module.exports = createLogger({
    level: "info",
    format: combine(
        label({ label: "Tenancy data migration" }),
        timestamp(),
        myFormat
    ),
    transports: [new transports.Console()]
});
