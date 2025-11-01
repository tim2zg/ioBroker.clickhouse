"use strict";

const utils = require("@iobroker/adapter-core");
const { createClient } = require("@clickhouse/client");

const SUBSCRIBE_THRESHOLD = 20;
const VALUE_TYPES = {
	NUMBER: "number",
	STRING: "string",
	BOOLEAN: "boolean",
	JSON: "json",
	NULL: "null",
};

const NUMERIC_EPSILON = 1e-12;

function isObject(value) {
	return Object.prototype.toString.call(value) === "[object Object]";
}

function extractError(error) {
	if (!error) {
		return "Unknown error";
	}
	if (typeof error === "string") {
		return error;
	}
	if (error instanceof Error) {
		return error.message;
	}
	if (isObject(error) && error.message) {
		return error.message;
	}
	try {
		return JSON.stringify(error);
	} catch (e) {
		return String(error);
	}
}

function parseNumber(value, defaultValue = 0) {
	if (typeof value === "number" && !isNaN(value)) {
		return value;
	}
	if (typeof value === "string" && value !== "") {
		const parsed = Number(value);
		return isNaN(parsed) ? defaultValue : parsed;
	}
	return defaultValue;
}

function parseBool(value, defaultValue = false) {
	if (value === undefined || value === null) {
		return defaultValue;
	}
	if (typeof value === "boolean") {
		return value;
	}
	if (typeof value === "string") {
		return value === "true" || value === "1" || value === "on";
	}
	return Boolean(value);
}

function formatDateTime(ts) {
	const date = new Date(ts);
	const iso = date.toISOString();
	return iso.replace("T", " ").replace("Z", "");
}

function makeComparableKey(value) {
	if (value === null || value === undefined) {
		return "__null__";
	}
	const type = typeof value;
	if (type === "number" || type === "boolean" || type === "bigint") {
		return `${type}:${value}`;
	}
	if (type === "string") {
		return `string:${value}`;
	}
	try {
		return `object:${JSON.stringify(value)}`;
	} catch (error) {
		return `object:${String(value)}`;
	}
}

function reduceOnChange(entries) {
	if (!entries.length) {
		return entries;
	}
	const result = [];
	let lastComparable;
	for (const entry of entries) {
		const comparable = makeComparableKey(entry.val);
		if (lastComparable === undefined || comparable !== lastComparable) {
			result.push(entry);
			lastComparable = comparable;
		}
	}
	return result;
}

class Clickhouse extends utils.Adapter {
	constructor(options = {}) {
		super({
			...options,
			name: "clickhouse",
		});

		this._client = null;
		this._buffer = [];
		this._flushPromise = null;
		this._bufferTimer = null;
		this._tracked = new Map();
		this._subscribeAll = false;
		this._connected = false;
		this._tableIdentifier = "";
		this._compactSchema = false;
		/** @type {{ host: string; port: number; secure: boolean; username: string; password: string; database: string; table: string; flushInterval: number; batchSize: number; connectTimeout: number }} */
		this._runtimeOptions = {
			host: "127.0.0.1",
			port: 8123,
			secure: false,
			username: "default",
			password: "",
			database: "iobroker",
			table: "history",
			flushInterval: 5000,
			batchSize: 500,
			connectTimeout: 10000,
		};
		this._defaults = {
			blockTime: 0,
			changesOnly: true,
			ignoreZero: false,
			ignoreBelowNumber: null,
			ignoreAboveNumber: null,
			round: null,
			changesRelogInterval: 0,
			changesMinDelta: 0,
			storageType: "auto",
			enableDebugLogs: false,
			disableSkippedValueLogging: false,
		};

		this.on("ready", this.onReady.bind(this));
		this.on("stateChange", this.onStateChange.bind(this));
		this.on("objectChange", this.onObjectChange.bind(this));
		this.on("message", this.onMessage.bind(this));
		this.on("unload", this.onUnload.bind(this));
	}

	parseAdapterConfig() {
		const host = String(this.config.host ?? "127.0.0.1").trim() || "127.0.0.1";
		const username = String(this.config.username ?? "default").trim() || "default";
		const password = this.config.password !== undefined ? String(this.config.password) : "";
		const database = String(this.config.database ?? "iobroker").trim() || "iobroker";
		const table = String(this.config.table ?? "history").trim() || "history";
		const port = Number(this.config.port ?? this._runtimeOptions.port);
		const flushInterval = Number(this.config.flushInterval ?? this._runtimeOptions.flushInterval);
		const batchSize = Number(this.config.batchSize ?? this._runtimeOptions.batchSize);
		const connectTimeout = Number(this.config.connectTimeout ?? this._runtimeOptions.connectTimeout);

		this.config.host = host;
		this.config.username = username;
		this.config.password = password;
		this.config.database = database;
		this.config.table = table;
		this.config.secure = parseBool(this.config.secure, false);
		this.config.port = String(port > 0 ? port : this._runtimeOptions.port);
		this.config.flushInterval = String(flushInterval > 0 ? flushInterval : this._runtimeOptions.flushInterval);
		this.config.batchSize = String(batchSize > 0 ? batchSize : this._runtimeOptions.batchSize);
		this.config.connectTimeout = String(connectTimeout > 0 ? connectTimeout : this._runtimeOptions.connectTimeout);

		this._runtimeOptions.host = host;
		this._runtimeOptions.username = username;
		this._runtimeOptions.password = password;
		this._runtimeOptions.database = database;
		this._runtimeOptions.table = table;
		this._runtimeOptions.secure = this.config.secure;
		this._runtimeOptions.port = port > 0 ? port : this._runtimeOptions.port;
		this._runtimeOptions.flushInterval = flushInterval > 0 ? flushInterval : this._runtimeOptions.flushInterval;
		this._runtimeOptions.batchSize = batchSize > 0 ? batchSize : this._runtimeOptions.batchSize;
		this._runtimeOptions.connectTimeout = connectTimeout > 0 ? connectTimeout : this._runtimeOptions.connectTimeout;

		this.log.debug(
			`Parsed adapter config: host=${this._runtimeOptions.host}:${this._runtimeOptions.port}, secure=${this._runtimeOptions.secure}, database=${this._runtimeOptions.database}, table=${this._runtimeOptions.table}, flushInterval=${this._runtimeOptions.flushInterval}, batchSize=${this._runtimeOptions.batchSize}`,
		);
	}

	async onReady() {
		try {
			this.parseAdapterConfig();
			await this.ensureInfoObjects();
			await this.ensureDefaultHistoryInstance();
			await this.connectToClickHouse();
			await this.initializeTrackedDatapoints();
			this.subscribeForeignObjects("*");
			this.log.info("ClickHouse adapter ready");
		} catch (error) {
			this.log.error(`Failed to start adapter: ${extractError(error)}`);
		}
	}

	async ensureInfoObjects() {
		await this.setObjectNotExistsAsync("info", {
			type: "channel",
			common: {
				name: "Information",
			},
			native: {},
		});

		await this.setObjectNotExistsAsync("info.connection", {
			type: "state",
			common: {
				name: "Connection",
				type: "boolean",
				role: "indicator.connected",
				read: true,
				write: false,
				def: false,
			},
			native: {},
		});

		await this.setStateAsync("info.connection", false, true).catch(error => {
			this.log.debug(`Cannot initialize info.connection: ${extractError(error)}`);
		});
	}

	async ensureDefaultHistoryInstance() {
		try {
			const systemConfig = await this.getForeignObjectAsync("system.config");
			if (systemConfig?.common && !systemConfig.common.defaultHistory) {
				systemConfig.common.defaultHistory = this.namespace;
				await this.setForeignObjectAsync("system.config", systemConfig);
				this.log.info(`Set default history instance to "${this.namespace}"`);
			}
		} catch (error) {
			this.log.debug(`Could not set default history instance: ${extractError(error)}`);
		}
	}

	quoteIdent(identifier) {
		return `\`${String(identifier).replace(/`/g, "``")}\``;
	}

	buildCreateTableQuery() {
		return `CREATE TABLE IF NOT EXISTS ${this._tableIdentifier}
{
	 id String,
	 ts DateTime64(3, 'UTC'),
	 val_float Nullable(Float64),
	 val_string Nullable(String),
	 val_bool Nullable(UInt8),
	 val_json Nullable(String),
	 q Int32
)
ENGINE = MergeTree()
ORDER BY (id, ts)`;
	}

	async alignTableSchema() {
		if (!this._client) {
			this._compactSchema = false;
			return;
		}
		try {
			const describeBefore = await this._client.query({
				query: `DESCRIBE TABLE ${this._tableIdentifier}`,
				format: "JSONEachRow",
			});
			const beforeRows = await describeBefore.json();
			const columnTypes = new Map(beforeRows.map(row => [row.name, row.type]));
			for (const column of ["lc", "type", "ack", "source"]) {
				if (columnTypes.has(column)) {
					await this._client.command({
						query: `ALTER TABLE ${this._tableIdentifier} DROP COLUMN ${this.quoteIdent(column)}`,
					});
				}
			}
			const desiredTypes = {
				val_float: "Nullable(Float64)",
				val_string: "Nullable(String)",
				val_bool: "Nullable(UInt8)",
				val_json: "Nullable(String)",
				q: "Int32",
			};
			for (const [column, type] of Object.entries(desiredTypes)) {
				const current = columnTypes.get(column);
				if (current && current !== type) {
					await this._client.command({
						query: `ALTER TABLE ${this._tableIdentifier} MODIFY COLUMN ${this.quoteIdent(column)} ${type}`,
					});
				}
			}
		} catch (error) {
			this.log.debug(`Table schema alignment skipped: ${extractError(error)}`);
		}
		try {
			const describeAfter = await this._client.query({
				query: `DESCRIBE TABLE ${this._tableIdentifier}`,
				format: "JSONEachRow",
			});
			const afterRows = await describeAfter.json();
			const columns = new Map(afterRows.map(row => [row.name, row.type]));
			const compact = !columns.has("lc") && !columns.has("type") && !columns.has("ack") && !columns.has("source") &&
				columns.get("val_float") === "Nullable(Float64)" &&
				columns.get("val_string") === "Nullable(String)" &&
				columns.get("val_bool") === "Nullable(UInt8)" &&
				columns.get("val_json") === "Nullable(String)" &&
				columns.get("q") === "Int32";
			this._compactSchema = compact;
		} catch (error) {
			this._compactSchema = false;
			this.log.debug(`Cannot verify table schema: ${extractError(error)}`);
		}
	}

	async connectToClickHouse() {
		await this.disconnectFromClickHouse();

		const hostUrl = `${this._runtimeOptions.secure ? "https" : "http"}://${this._runtimeOptions.host}:${this._runtimeOptions.port}`;
		this.log.debug(`Connecting to ClickHouse at ${hostUrl}, database=${this._runtimeOptions.database}`);
		const adminClient = createClient({
			host: hostUrl,
			username: this._runtimeOptions.username,
			password: this._runtimeOptions.password,
			request_timeout: this._runtimeOptions.connectTimeout,
		});

		try {
			await adminClient.command({
				query: `CREATE DATABASE IF NOT EXISTS ${this.quoteIdent(this.config.database)}`,
			});
		} finally {
			await adminClient.close().catch(() => null);
		}

		this._client = createClient({
			host: hostUrl,
			database: this._runtimeOptions.database,
			username: this._runtimeOptions.username,
			password: this._runtimeOptions.password,
			request_timeout: this._runtimeOptions.connectTimeout,
		});

		this._tableIdentifier = this.quoteIdent(this._runtimeOptions.table);
		await this._client.command({ query: this.buildCreateTableQuery() });
		await this.alignTableSchema();
		this.setConnected(true);
		this.log.debug(`Connected to ClickHouse; using table ${this._runtimeOptions.database}.${this._runtimeOptions.table}`);
		this.startFlushTimer();
	}

	async disconnectFromClickHouse() {
		if (this._client) {
			await this._client.close().catch(() => null);
			this._client = null;
		}
		this.setConnected(false);
	}

	startFlushTimer() {
		this.stopFlushTimer();
		this._bufferTimer = setInterval(() => {
			void this.flushBuffer().catch(error => {
				this.log.warn(`Automatic buffer flush failed: ${extractError(error)}`);
			});
		}, this._runtimeOptions.flushInterval);
		if (typeof this._bufferTimer.unref === "function") {
			this._bufferTimer.unref();
		}
	}

	stopFlushTimer() {
		if (this._bufferTimer) {
			clearInterval(this._bufferTimer);
			this._bufferTimer = null;
		}
	}

	async initializeTrackedDatapoints() {
		this._tracked.clear();
		try {
			const doc = await this.getObjectViewAsync("system", "custom", {});
			if (doc?.rows?.length) {
				for (const row of doc.rows) {
					const custom = row.value?.[this.namespace];
					if (custom?.enabled) {
						await this.addTrackedDatapoint(row.id, custom);
					}
				}
			}
		} catch (error) {
			this.log.warn(`Could not load custom settings: ${extractError(error)}`);
		}

		if (this._tracked.size >= SUBSCRIBE_THRESHOLD && !this._subscribeAll) {
			this._subscribeAll = true;
			this.subscribeForeignStates("*");
		} else if (!this._subscribeAll) {
			for (const id of Array.from(this._tracked.keys())) {
				this.subscribeForeignStates(id);
			}
		}
	}

	normalizeStateConfig(custom = {}) {
		const normalized = {
			storageType: String(custom.storageType || this._defaults.storageType).toLowerCase(),
			blockTime: Math.max(parseNumber(custom.blockTime, this._defaults.blockTime), 0),
			changesOnly: parseBool(custom.changesOnly, this._defaults.changesOnly),
			ignoreZero: parseBool(custom.ignoreZero, this._defaults.ignoreZero),
			ignoreBelowNumber:
				custom.ignoreBelowNumber !== "" && custom.ignoreBelowNumber !== undefined
					? Number(custom.ignoreBelowNumber)
					: this._defaults.ignoreBelowNumber,
			ignoreAboveNumber:
				custom.ignoreAboveNumber !== "" && custom.ignoreAboveNumber !== undefined
					? Number(custom.ignoreAboveNumber)
					: this._defaults.ignoreAboveNumber,
			round:
				custom.round !== "" && custom.round !== undefined
					? Math.max(parseInt(custom.round, 10) || 0, 0)
					: null,
			changesRelogInterval: Math.max(parseNumber(custom.changesRelogInterval, this._defaults.changesRelogInterval), 0),
			changesMinDelta: Math.max(parseNumber(custom.changesMinDelta, this._defaults.changesMinDelta), 0),
			enableDebugLogs: parseBool(custom.enableDebugLogs, this._defaults.enableDebugLogs),
			disableSkippedValueLogging: parseBool(
				custom.disableSkippedValueLogging,
				this._defaults.disableSkippedValueLogging,
			),
		};
		if (isNaN(normalized.ignoreBelowNumber)) {
			normalized.ignoreBelowNumber = null;
		}
		if (isNaN(normalized.ignoreAboveNumber)) {
			normalized.ignoreAboveNumber = null;
		}
		return normalized;
	}

	async addTrackedDatapoint(id, custom) {
		const normalized = this.normalizeStateConfig(custom);
		const entry = {
			id,
			config: normalized,
			configHash: JSON.stringify(custom),
			timeout: null,
			relogTimeout: null,
			lastState: null,
			lastStoredState: null,
			lastStoredComparable: undefined,
			lastLogTime: 0,
			lastSkippedState: null,
			ephemeral: false,
		};
		this._tracked.set(id, entry);

		if (!this._subscribeAll && this._tracked.size < SUBSCRIBE_THRESHOLD) {
			this.subscribeForeignStates(id);
		} else if (!this._subscribeAll && this._tracked.size >= SUBSCRIBE_THRESHOLD) {
			this._subscribeAll = true;
			this.subscribeForeignStates("*");
		}

		try {
			const current = await this.getForeignStateAsync(id);
			if (current) {
				entry.lastState = { ...current };
			}
		} catch (error) {
			this.log.debug(`Cannot read initial state for ${id}: ${extractError(error)}`);
		}

		this.log.info(`Enabled logging for ${id}`);
		return entry;
	}

	createEphemeralEntry(id) {
		const entry = {
			id,
			config: { ...this._defaults },
			configHash: null,
			timeout: null,
			relogTimeout: null,
			lastState: null,
			lastStoredState: null,
			lastStoredComparable: undefined,
			lastLogTime: 0,
			lastSkippedState: null,
			ephemeral: true,
		};
		this._tracked.set(id, entry);
		if (!this._subscribeAll && this._tracked.size < SUBSCRIBE_THRESHOLD) {
			this.subscribeForeignStates(id);
		}
		return entry;
	}

	removeTrackedDatapoint(id) {
		const entry = this._tracked.get(id);
		if (!entry) {
			return;
		}
		if (entry.timeout) {
			clearTimeout(entry.timeout);
			entry.timeout = null;
		}
		if (entry.relogTimeout) {
			clearTimeout(entry.relogTimeout);
			entry.relogTimeout = null;
		}
		this._tracked.delete(id);
		if (!this._subscribeAll) {
			this.unsubscribeForeignStates(id);
		}
		this.log.info(`Disabled logging for ${id}`);
	}

	async onObjectChange(id, obj) {
		try {
			const custom = obj?.common?.custom?.[this.namespace];
			if (!custom || !custom.enabled) {
				this.removeTrackedDatapoint(id);
				return;
			}

			const hash = JSON.stringify(custom);
			const entry = this._tracked.get(id);
			if (entry && entry.configHash === hash) {
				return;
			}

			this.removeTrackedDatapoint(id);
			await this.addTrackedDatapoint(id, custom);
		} catch (error) {
			this.log.error(`Error handling object change for ${id}: ${extractError(error)}`);
		}
	}

	onStateChange(id, state) {
		if (!state) {
			return;
		}
		void this.pushHistory(id, state).catch(error => {
			this.log.error(`Could not store state for ${id}: ${extractError(error)}`);
		});
	}

	prepareValue(value, settings) {
		if (value === null || value === undefined) {
			return { type: VALUE_TYPES.NULL, value: null, comparable: null };
		}

		const storageType = settings.storageType || "auto";
		const lower = storageType.toLowerCase();

		const roundDigits = typeof settings.round === "number" ? settings.round : null;

		const toRoundedNumber = number => {
			if (!isFinite(number)) {
				throw new Error("Non finite number value");
			}
			if (roundDigits !== null) {
				const factor = Math.pow(10, roundDigits);
				return Math.round(number * factor) / factor;
			}
			return number;
		};

		const autoDetect = val => {
			if (typeof val === "number") {
				return {
					type: VALUE_TYPES.NUMBER,
					value: toRoundedNumber(val),
					comparable: toRoundedNumber(val),
				};
			}
			if (typeof val === "boolean") {
				return { type: VALUE_TYPES.BOOLEAN, value: val, comparable: val };
			}
			if (typeof val === "string") {
				return { type: VALUE_TYPES.STRING, value: val, comparable: val };
			}
			if (typeof val === "object") {
				return {
					type: VALUE_TYPES.JSON,
					value: JSON.stringify(val),
					comparable: JSON.stringify(val),
				};
			}
			return { type: VALUE_TYPES.STRING, value: String(val), comparable: String(val) };
		};

		switch (lower) {
			case "number": {
				const numeric = Number(value);
				if (!isFinite(numeric)) {
					throw new Error("Cannot convert value to number");
				}
				const rounded = toRoundedNumber(numeric);
				return { type: VALUE_TYPES.NUMBER, value: rounded, comparable: rounded };
			}
			case "string": {
				const asString = value === null || value === undefined ? "" : String(value);
				return { type: VALUE_TYPES.STRING, value: asString, comparable: asString };
			}
			case "boolean": {
				return { type: VALUE_TYPES.BOOLEAN, value: Boolean(value), comparable: Boolean(value) };
			}
			case "json": {
				const serialized = typeof value === "string" ? value : JSON.stringify(value);
				return { type: VALUE_TYPES.JSON, value: serialized, comparable: serialized };
			}
			default:
				return autoDetect(value);
		}
	}

	valuesEqual(converted, entry) {
		if (!entry.lastStoredState) {
			return false;
		}
		if (converted.type !== entry.lastStoredState.type) {
			return false;
		}
		switch (converted.type) {
			case VALUE_TYPES.NUMBER:
				return typeof entry.lastStoredComparable === "number"
					? Math.abs(entry.lastStoredComparable - converted.comparable) < NUMERIC_EPSILON
					: false;
			case VALUE_TYPES.BOOLEAN:
				return entry.lastStoredComparable === converted.comparable;
			case VALUE_TYPES.STRING:
			case VALUE_TYPES.JSON:
				return entry.lastStoredComparable === converted.comparable;
			case VALUE_TYPES.NULL:
				return entry.lastStoredComparable === null && converted.comparable === null;
			default:
				return false;
		}
	}

	async ensureEntryForId(id) {
		let entry = this._tracked.get(id);
		if (entry) {
			return entry;
		}
		try {
			const obj = await this.getForeignObjectAsync(id);
			const custom = obj?.common?.custom?.[this.namespace];
			if (custom?.enabled) {
				entry = await this.addTrackedDatapoint(id, custom);
				return entry;
			}
		} catch (error) {
			this.log.debug(`Cannot resolve object config for ${id}: ${extractError(error)}`);
		}
		return this.createEphemeralEntry(id);
	}

	shouldSkipValue(entry, converted, state, timerRelog) {
		const settings = entry.config;
		const lastStored = entry.lastStoredState;

		if (!timerRelog && settings.blockTime > 0 && lastStored) {
			if (state.ts <= lastStored.ts + settings.blockTime) {
				settings.enableDebugLogs &&
					this.log.debug(`Skip ${entry.id}: blockTime active (${settings.blockTime}ms)`);
				return true;
			}
		}

		if (!timerRelog && settings.ignoreZero) {
			if (
				converted.type === VALUE_TYPES.NUMBER &&
				converted.comparable === 0
			) {
				settings.enableDebugLogs && this.log.debug(`Skip ${entry.id}: ignoreZero`);
				return true;
			}
			if (converted.type === VALUE_TYPES.NULL) {
				settings.enableDebugLogs && this.log.debug(`Skip ${entry.id}: null value`);
				return true;
			}
		}

		if (
			settings.ignoreBelowNumber !== null &&
			converted.type === VALUE_TYPES.NUMBER &&
			converted.comparable < settings.ignoreBelowNumber
		) {
			settings.enableDebugLogs &&
				this.log.debug(`Skip ${entry.id}: below threshold ${settings.ignoreBelowNumber}`);
			return true;
		}

		if (
			settings.ignoreAboveNumber !== null &&
			converted.type === VALUE_TYPES.NUMBER &&
			converted.comparable > settings.ignoreAboveNumber
		) {
			settings.enableDebugLogs &&
				this.log.debug(`Skip ${entry.id}: above threshold ${settings.ignoreAboveNumber}`);
			return true;
		}

		let valueChanged = !this.valuesEqual(converted, entry);
		if (valueChanged && converted.type === VALUE_TYPES.NUMBER && entry.lastStoredComparable !== undefined) {
			if (
				settings.changesMinDelta > 0 &&
				Math.abs(converted.comparable - entry.lastStoredComparable) < settings.changesMinDelta
			) {
				valueChanged = false;
			}
		}

		if (!timerRelog && settings.changesOnly && entry.lastStoredState) {
			if (!valueChanged) {
				const relogMs = settings.changesRelogInterval > 0 ? settings.changesRelogInterval * 1000 : 0;
				if (relogMs > 0 && (!entry.lastLogTime || state.ts - entry.lastLogTime >= relogMs)) {
					settings.enableDebugLogs &&
						this.log.debug(
							`Relog interval reached for ${entry.id} after ${state.ts - entry.lastLogTime}ms`,
						);
					return false;
				}
				if (!settings.disableSkippedValueLogging) {
					entry.lastSkippedState = { ...state };
				}
				settings.enableDebugLogs && this.log.debug(`Skip ${entry.id}: value unchanged`);
				return true;
			}
		}

		return false;
	}

	buildRow(id, converted, state) {
		if (this._compactSchema) {
			return {
				id,
				ts: formatDateTime(state.ts),
				val_float: converted.type === VALUE_TYPES.NUMBER ? converted.value : null,
				val_string: converted.type === VALUE_TYPES.STRING ? converted.value : null,
				val_bool:
					converted.type === VALUE_TYPES.BOOLEAN ? (converted.value ? 1 : 0) : null,
				val_json: converted.type === VALUE_TYPES.JSON ? converted.value : null,
				q: state.q !== undefined ? state.q : 0,
			};
		}

		return {
			id,
			ts: formatDateTime(state.ts),
			lc: formatDateTime(state.lc),
			type: converted.type,
			val_float: converted.type === VALUE_TYPES.NUMBER ? converted.value : 0,
			val_string: converted.type === VALUE_TYPES.STRING ? converted.value : "",
			val_bool: converted.type === VALUE_TYPES.BOOLEAN ? (converted.value ? 1 : 0) : 0,
			val_json: converted.type === VALUE_TYPES.JSON ? converted.value : "",
			ack: state.ack ? 1 : 0,
			q: state.q !== undefined ? state.q : 0,
			source: state.from || "",
		};
	}

	async queueRow(row) {
		this._buffer.push(row);
		const size = this._buffer.length;
		if (size === 1 || size >= this._runtimeOptions.batchSize || size % 50 === 0) {
			this.log.debug(`Queued row for ${row.id}; buffer size now ${size}`);
		}
		if (this._buffer.length >= this._runtimeOptions.batchSize) {
			await this.flushBuffer();
		}
	}

	async pushHistory(id, state, timerRelog = false, allowCreate = false, suppressDebug = false) {
		let entry = this._tracked.get(id);
		if (!entry) {
			if (!allowCreate) {
				return;
			}
			entry = await this.ensureEntryForId(id);
		}

		const settings = entry.config;
		const clonedState = {
			val: state.val,
			ts: typeof state.ts === "number" && !isNaN(state.ts) ? state.ts : Date.now(),
			lc: typeof state.lc === "number" && !isNaN(state.lc) ? state.lc : Date.now(),
			ack: state.ack ?? false,
			q: state.q ?? 0,
			from: state.from || "",
		};

		if (clonedState.val === undefined) {
			this.log.warn(`Value for ${id} is undefined and cannot be stored`);
			return;
		}

		entry.lastState = { ...clonedState };

		let converted;
		try {
			converted = this.prepareValue(clonedState.val, settings);
		} catch (error) {
			this.log.warn(`Cannot store value for ${id}: ${extractError(error)}`);
			return;
		}

		if (this.shouldSkipValue(entry, converted, clonedState, timerRelog)) {
			return;
		}

		const row = this.buildRow(id, converted, clonedState);
		await this.queueRow(row);

		entry.lastStoredState = {
			val: converted.value,
			ts: clonedState.ts,
			lc: clonedState.lc,
			type: converted.type,
			ack: clonedState.ack,
			q: clonedState.q,
			source: clonedState.from,
		};
		entry.lastStoredComparable = converted.comparable;
		entry.lastLogTime = clonedState.ts;
		entry.lastSkippedState = null;

		if (entry.relogTimeout) {
			clearTimeout(entry.relogTimeout);
			entry.relogTimeout = null;
		}

		if (settings.changesRelogInterval > 0) {
			entry.relogTimeout = setTimeout(() => {
				entry.relogTimeout = null;
				if (!entry.lastStoredState) {
					return;
				}
				const relogState = {
					val: entry.lastStoredState.val,
					ts: Date.now(),
					lc: entry.lastStoredState.lc || Date.now(),
					ack: entry.lastStoredState.ack,
					q: entry.lastStoredState.q,
					from: entry.lastStoredState.source,
				};
				void this.pushHistory(id, relogState, true, true).catch(error => {
					this.log.debug(`Relog for ${id} failed: ${extractError(error)}`);
				});
			}, settings.changesRelogInterval * 1000);
			if (typeof entry.relogTimeout.unref === "function") {
				entry.relogTimeout.unref();
			}
		}

		if (!suppressDebug && settings.enableDebugLogs) {
			this.log.debug(
				`Stored value for ${id}: ${JSON.stringify({ type: converted.type, value: converted.value, ts: clonedState.ts })}`,
			);
		}
	}

	async flushBuffer(force = false) {
		if (!this._client) {
			return 0;
		}
		if (!this._buffer.length && !force) {
			return 0;
		}
		if (this._flushPromise && !force) {
			return this._flushPromise;
		}
		if (this._flushPromise && force) {
			await this._flushPromise.catch(() => null);
		}

		if (!this._buffer.length) {
			return 0;
		}

		const rows = this._buffer.splice(0);
		this._flushPromise = (async () => {
			try {
				await this._client.insert({
					table: this._runtimeOptions.table,
					values: rows,
					format: "JSONEachRow",
				});
				this.setConnected(true);
				this.log.debug(`Flushed ${rows.length} rows to ClickHouse; remaining buffer=${this._buffer.length}`);
				return rows.length;
			} catch (error) {
				this.setConnected(false);
				this.log.error(`Failed to write ${rows.length} rows: ${extractError(error)}`);
				this._buffer = rows.concat(this._buffer);
				throw error;
			} finally {
				this._flushPromise = null;
			}
		})();

		return this._flushPromise;
	}

	setConnected(isConnected) {
		if (this._connected === isConnected) {
			return;
		}
		this._connected = isConnected;
		void this.setStateAsync("info.connection", isConnected, true).catch(error => {
			this.log.error(`Cannot update info.connection: ${extractError(error)}`);
		});
	}

	async onUnload(callback) {
		try {
			this.stopFlushTimer();
			await this.flushBuffer(true).catch(() => null);
			await this.disconnectFromClickHouse();
		} catch (error) {
			this.log.error(`Error during unload: ${extractError(error)}`);
		} finally {
			callback();
		}
	}

	async onMessage(msg) {
		this.log.debug(`Incoming message ${msg.command}`);
		try {
			switch (msg.command) {
				case "features":
					this.sendTo(
						msg.from,
						msg.command,
						{ supportedFeatures: ["update", "delete", "deleteRange", "deleteAll", "storeState"] },
						msg.callback,
					);
					break;
				case "storeState":
					await this.handleStoreState(msg);
					break;
				case "update":
					await this.handleUpdateState(msg);
					break;
				case "delete":
					await this.handleDelete(msg);
					break;
				case "deleteRange":
					await this.handleDeleteRange(msg);
					break;
				case "deleteAll":
					await this.handleDeleteAll(msg);
					break;
				case "getHistory":
					await this.handleGetHistory(msg);
					break;
				case "enableHistory":
					await this.handleEnableHistory(msg);
					break;
				case "disableHistory":
					await this.handleDisableHistory(msg);
					break;
				case "getEnabledDPs":
					this.handleGetEnabledDPs(msg);
					break;
				case "flushBuffer":
					await this.flushBuffer(true);
					if (msg.callback) {
						this.sendTo(msg.from, msg.command, { error: null }, msg.callback);
					}
					break;
				case "test":
					await this.handleTestConnection(msg);
					break;
				default:
					this.log.warn(`Unsupported message command ${msg.command}`);
					if (msg.callback) {
						this.sendTo(
							msg.from,
							msg.command,
							{ error: `Unsupported command ${msg.command}` },
							msg.callback,
						);
					}
					break;
			}
		} catch (error) {
			this.log.error(`Cannot process message ${msg.command}: ${extractError(error)}`);
			if (msg.callback) {
				this.sendTo(msg.from, msg.command, { error: extractError(error) }, msg.callback);
			}
		}
	}

	async handleStoreState(msg) {
		const id = msg.message?.id;
		const state = msg.message?.state;
		if (!id || !isObject(state)) {
			throw new Error("storeState called with invalid payload");
		}
		await this.pushHistory(id, state, false, true);
		if (msg.message?.flush) {
			await this.flushBuffer(true);
		}
		if (msg.callback) {
			this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
		}
	}

	async handleUpdateState(msg) {
		const id = msg.message?.id;
		const state = msg.message?.state;
		if (!id || !isObject(state) || typeof state.ts !== "number") {
			throw new Error("update called with invalid payload");
		}
		await this.executeDelete(id, state.ts, state.ts);
		await this.pushHistory(id, state, false, true);
		if (msg.callback) {
			this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
		}
	}

	async handleDelete(msg) {
		const id = msg.message?.id;
		const ts = msg.message?.ts;
		if (!id || ts === undefined) {
			throw new Error("delete called without id or ts");
		}
		if (Array.isArray(ts)) {
			for (const single of ts) {
				await this.executeDelete(id, single, single);
			}
		} else {
			await this.executeDelete(id, ts, ts);
		}
		if (msg.callback) {
			this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
		}
	}

	async handleDeleteRange(msg) {
		const id = msg.message?.id;
		const start = msg.message?.start;
		const end = msg.message?.end;
		if (!id || start === undefined || end === undefined) {
			throw new Error("deleteRange requires id, start and end");
		}
		await this.executeDelete(id, start, end);
		if (msg.callback) {
			this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
		}
	}

	async handleDeleteAll(msg) {
		const id = msg.message?.id;
		if (!id) {
			throw new Error("deleteAll requires id");
		}
		await this.executeDelete(id);
		if (msg.callback) {
			this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
		}
	}

	async executeDelete(id, start, end) {
		if (!this._client) {
			throw new Error("Not connected to ClickHouse");
		}
		await this.flushBuffer(true).catch(() => null);
		const parameters = { id: String(id) };
		const conditions = ["id = {id:String}"];
		if (start !== undefined && end !== undefined) {
			parameters.start = Number(start);
			parameters.end = Number(end);
			conditions.push(
				"ts BETWEEN fromUnixTimestamp64Milli({start:UInt64}) AND fromUnixTimestamp64Milli({end:UInt64})",
			);
		}
		const query = `ALTER TABLE ${this._tableIdentifier} DELETE WHERE ${conditions.join(" AND ")}`;
		await this._client.command({ query, query_params: parameters });
	}

	async handleGetHistory(msg) {
		const id = msg.message?.id;
		const options = msg.message?.options || {};
		if (!id || !isObject(options)) {
			throw new Error("getHistory called with invalid payload");
		}
		this.log.debug(
			`History request for ${id} with options ${JSON.stringify({ ...options, password: undefined })}`,
		);

		const aggregate = options.aggregate || "none";
		if (aggregate !== "none" && aggregate !== "onchange") {
			throw new Error(`Aggregation ${aggregate} is not supported`);
		}

		await this.flushBuffer(true).catch(() => null);

		const params = { id: String(id) };
		const where = ["id = {id:String}"];

		if (options.start !== undefined) {
			const start = typeof options.start === "number" ? options.start : new Date(options.start).getTime();
			if (!isNaN(start)) {
				params.start = Number(start);
				where.push("ts >= fromUnixTimestamp64Milli({start:UInt64})");
			}
		}
		if (options.end !== undefined) {
			const end = typeof options.end === "number" ? options.end : new Date(options.end).getTime();
			if (!isNaN(end)) {
				params.end = Number(end);
				where.push("ts <= fromUnixTimestamp64Milli({end:UInt64})");
			}
		}

		const limit = parseInt(options.limit, 10) || parseInt(options.count, 10) || 2000;
		if (limit > 0) {
			params.limit = limit;
		}

		const order = options.returnNewestEntries ? "DESC" : "ASC";
		let selectClause;
		if (this._compactSchema) {
			selectClause = `SELECT
				toUnixTimestamp64Milli(ts) AS ts,
				toUnixTimestamp64Milli(ts) AS lc,
				multiIf(
					val_json IS NOT NULL, '${VALUE_TYPES.JSON}',
					val_string IS NOT NULL, '${VALUE_TYPES.STRING}',
					val_bool IS NOT NULL, '${VALUE_TYPES.BOOLEAN}',
					val_float IS NOT NULL, '${VALUE_TYPES.NUMBER}',
					'${VALUE_TYPES.NULL}'
				) AS type,
				val_float,
				val_string,
				val_bool,
				val_json,
				q,
				1 AS ack,
				'' AS source
			`;
		} else {
			selectClause = `SELECT
				toUnixTimestamp64Milli(ts) AS ts,
				toUnixTimestamp64Milli(lc) AS lc,
				type,
				val_float,
				val_string,
				val_bool,
				val_json,
				ack,
				q,
				source
			`;
		}

		const query = `${selectClause}
		FROM ${this._tableIdentifier}
		WHERE ${where.join(" AND ")}
		ORDER BY ts ${order}
		${limit > 0 ? "LIMIT {limit:UInt32}" : ""}`;

		if (!this._client) {
			throw new Error("Not connected to ClickHouse");
		}

		const result = await this._client.query({ query, query_params: params, format: "JSONEachRow" });
		const rows = await result.json();

		const data = rows.map(row => this.mapRowToHistory(row, id, options.addId));
		const filtered = options.ignoreNull === false ? data : data.filter(item => item.val !== null);
		let resultData = filtered;
		if (aggregate === "onchange") {
			resultData = reduceOnChange(filtered);
		}
		this.log.debug(`History response for ${id}: returned ${resultData.length} data points`);

		if (msg.callback) {
			this.sendTo(msg.from, msg.command, { result: resultData, step: null, error: null }, msg.callback);
		}
	}

	mapRowToHistory(row, id, addId) {
		const ts = Number(row.ts);
		const lc = Number(row.lc);
		let val = null;
		switch (row.type) {
			case VALUE_TYPES.NUMBER:
				val = row.val_float;
				break;
			case VALUE_TYPES.BOOLEAN:
				val = !!row.val_bool;
				break;
			case VALUE_TYPES.STRING:
				val = row.val_string;
				break;
			case VALUE_TYPES.JSON:
				try {
					val = JSON.parse(row.val_json || "null");
				} catch (error) {
					this.log.debug(`Cannot parse JSON for ${id}: ${extractError(error)}`);
					val = row.val_json;
				}
				break;
			default:
				val = null;
				break;
		}

		const entry = {
			val,
			ts: isNaN(ts) ? Date.now() : ts,
			lc: isNaN(lc) ? undefined : lc,
			ack: !!row.ack,
			q: row.q ?? 0,
			from: row.source || "",
		};

		if (addId) {
			entry.id = id;
		}

		return entry;
	}

	async handleEnableHistory(msg) {
		const id = msg.message?.id;
		const options = msg.message?.options;
		if (!id || !isObject(options)) {
			throw new Error("enableHistory requires id and options");
		}
		const obj = await this.getForeignObjectAsync(id);
		if (!obj?.common) {
			throw new Error(`Object ${id} not found`);
		}
		obj.common.custom = obj.common.custom || {};
		obj.common.custom[this.namespace] = { ...options, enabled: true };
		await this.setForeignObjectAsync(id, obj);
		if (msg.callback) {
			this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
		}
	}

	async handleDisableHistory(msg) {
		const id = msg.message?.id;
		if (!id) {
			throw new Error("disableHistory requires id");
		}
		const obj = await this.getForeignObjectAsync(id);
		if (!obj?.common?.custom?.[this.namespace]) {
			return;
		}
		delete obj.common.custom[this.namespace];
		await this.setForeignObjectAsync(id, obj);
		if (msg.callback) {
			this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
		}
	}

	handleGetEnabledDPs(msg) {
		const result = {};
		for (const [id, entry] of Array.from(this._tracked.entries())) {
			if (!entry.ephemeral) {
				result[id] = entry.config;
			}
		}
		if (msg.callback) {
			this.sendTo(msg.from, msg.command, result, msg.callback);
		}
	}

	async handleTestConnection(msg) {
		const testConfig = msg.message?.config;
		const config = {
			host: String(testConfig?.host ?? this._runtimeOptions.host ?? "127.0.0.1").trim() || "127.0.0.1",
			port: Number(testConfig?.port ?? this._runtimeOptions.port ?? 8123) || 8123,
			secure: parseBool(testConfig?.secure ?? this._runtimeOptions.secure, false),
			username: String(testConfig?.username ?? this._runtimeOptions.username ?? "default").trim() || "default",
			password: testConfig?.password ?? this._runtimeOptions.password ?? "",
			database: String(testConfig?.database ?? this._runtimeOptions.database ?? "iobroker").trim() || "iobroker",
			table: String(testConfig?.table ?? this._runtimeOptions.table ?? "history").trim() || "history",
			connectTimeout: Number(
				testConfig?.connectTimeout ?? this._runtimeOptions.connectTimeout ?? 10000,
			) || 10000,
		};
		this.log.debug(
			`Testing ClickHouse connection with host=${config.host}:${config.port}, secure=${config.secure}, database=${config.database}, table=${config.table}`,
		);

		const hostUrl = `${config.secure ? "https" : "http"}://${config.host}:${config.port}`;
		const adminClient = createClient({
			host: hostUrl,
			username: config.username,
			password: config.password,
			request_timeout: config.connectTimeout,
		});
		let dataClient = null;
		try {
			await adminClient.command({
				query: `CREATE DATABASE IF NOT EXISTS ${this.quoteIdent(config.database)}`,
			});
			dataClient = createClient({
				host: hostUrl,
				database: config.database,
				username: config.username,
				password: config.password,
				request_timeout: config.connectTimeout,
			});
			await dataClient.query({ query: "SELECT 1", format: "JSONEachRow" }).then(result => result.json());
			if (msg.callback) {
				this.sendTo(msg.from, msg.command, { error: null }, msg.callback);
			}
		} catch (error) {
			if (msg.callback) {
				this.sendTo(msg.from, msg.command, { error: extractError(error) }, msg.callback);
			}
		} finally {
			await adminClient.close().catch(() => null);
			if (dataClient) {
				await dataClient.close().catch(() => null);
			}
		}
	}
}

if (require.main !== module) {
	module.exports = options => new Clickhouse(options);
} else {
	new Clickhouse();
}