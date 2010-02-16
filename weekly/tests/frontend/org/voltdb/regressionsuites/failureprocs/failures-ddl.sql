CREATE TABLE NEW_ORDER (
  NO_O_ID INTEGER DEFAULT '0' NOT NULL,
  NO_D_ID TINYINT DEFAULT '0' NOT NULL,
  NO_W_ID TINYINT DEFAULT '0' NOT NULL,
  CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID)
);

CREATE TABLE FIVEK_STRING (
  ID INTEGER DEFAULT '0' NOT NULL,
  P INTEGER DEFAULT '0' NOT NULL,
  CVALUE VARCHAR(30000) DEFAULT '' NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE FIVEK_STRING_WITH_INDEX (
  ID INTEGER DEFAULT '0' NOT NULL,
  CVALUE VARCHAR(5000) DEFAULT '' NOT NULL,
  PRIMARY KEY (ID),
  UNIQUE (CVALUE)
);

CREATE TABLE WIDE (
    ID INTEGER DEFAULT '0' NOT NULL,
    P INTEGER DEFAULT '0' NOT NULL,
    CVALUE1 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE2 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE3 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE4 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE5 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE6 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE7 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE8 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE9 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE10 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE11 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE12 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE13 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE14 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE15 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE16 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE17 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE18 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE19 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE20 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE21 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE22 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE23 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE24 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE25 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE26 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE27 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE28 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE29 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE30 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE31 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE32 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE33 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE34 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE35 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE36 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE37 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE38 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE39 VARCHAR(254) DEFAULT '' NOT NULL,
    CVALUE40 VARCHAR(254) DEFAULT '' NOT NULL,
);
