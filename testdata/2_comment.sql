--UP--
CREATE TABLE test2
(
    id   bigint    NOT NULL,
    time timestamp NOT NULL,
    PRIMARY KEY (id)
);
--DOWN--
DROP TABLE test2;
--SNAPSHOT--
CREATE TABLE test
(
    id   bigint    NOT NULL,
    time timestamp NOT NULL,
    PRIMARY KEY (id)
);
CREATE TABLE test2
(
    id   bigint    NOT NULL,
    time timestamp NOT NULL,
    PRIMARY KEY (id)
);
