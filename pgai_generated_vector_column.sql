CREATE EXTENSION IF NOT EXISTS ai cascade;

CREATE FUNCTION embed(text) RETURNS VECTOR AS $$
BEGIN
  return openai_embed('text-embedding-ada-002', $1);
END $$ IMMUTABLE LANGUAGE plpgsql;

DROP TABLE IF EXISTS embeddings;
CREATE TABLE embeddings (
  content TEXT,
  vec VECTOR GENERATED ALWAYS AS (embed(content)) STORED
);

INSERT INTO embeddings (content) VALUES
    ('Apple'),
    ('Avocado'),
    ('Banana'),
    ('Pineapple'),
    ('Strawberry'),
    ('Macbook'),
    ('iPhone'),
    ('Samsung Galaxy'),
    ('Playstation'),
    ('Nintendo Switch')
;

SELECT content
FROM embeddings
WHERE    vec <-> embed('Fruits') < 0.6
ORDER BY vec <-> embed('soft texture')
LIMIT 3;

