/*
   Everything before --UP-- must be ignored
 */

SELECT *
FROM test;

--UP--

CREATE TABLE test
(
    id   bigint    NOT NULL,
    time timestamp NOT NULL,
    PRIMARY KEY (id)
);

--DOWN--

DROP TABLE test;
