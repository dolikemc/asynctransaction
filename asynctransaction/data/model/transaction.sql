CREATE TABLE EVENTS (
    ID integer primary key,
    URL text,
    METHOD text,
    DESCRIPTION text,
    CREATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    UPDATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    DELETED integer default(0)
 );

CREATE TABLE PARTNERS (
    ID integer primary key,
    IP_ADDRESS TEXT,
    PORT integer,
    DESCRIPTION text,
    CREATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    UPDATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    DELETED integer default(0)
);

CREATE TABLE SUBSCRIBERS (
    ID integer primary key,
    EVENT_ID integer,
    PARTNER_ID integer,
    CREATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    UPDATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    DELETED integer default(0),
    foreign key(EVENT_ID) REFERENCES EVENTS(ID),
    foreign key(PARTNER_ID) REFERENCES PARTNERS(ID)
);

CREATE INDEX SUBSCRIBER_EVENT_FK ON SUBSCRIBERS(EVENT_ID);
CREATE INDEX SUBSCRIBER_PARTNER_FK ON SUBSCRIBERS(PARTNER_ID);

CREATE TABLE TASKS (
    ID integer primary key,
    LOCAL_ID integer,
    PARTNER_ID integer,
    EVENT_ID integer,
    DATA TEXT,
    STATE INTEGER DEFAULT(1),
    CREATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    UPDATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    DELETED integer default(0),
    CONSTRAINT FK_EVENTS foreign key(EVENT_ID) REFERENCES EVENTS(ID),
    CONSTRAINT FK_PARTNERS foreign key(PARTNER_ID) REFERENCES PARTNERS(ID)
);

CREATE INDEX TASK_EVENT_FK ON TASKS(EVENT_ID);
CREATE INDEX TASK_PARTNER_FK ON TASKS(PARTNER_ID);

CREATE TABLE PROCESSING_STEPS (
    ID integer primary key,
    TASK_ID integer,
    PARTNER_ID integer,
    STATE INTEGER DEFAULT(1),
    CREATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    UPDATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    DELETED integer default(0),
    CONSTRAINT FK_TASKS foreign key(TASK_ID) REFERENCES TASKS(ID),
    CONSTRAINT FK_SUBSCRIBERS foreign key(PARTNER_ID) REFERENCES PARTNERS(ID)
);

CREATE INDEX PROCESS_STATUS_FK ON PROCESSING_STEPS(STATE);
CREATE INDEX PROCESS_PARTNER_FK ON PROCESSING_STEPS(PARTNER_ID);
