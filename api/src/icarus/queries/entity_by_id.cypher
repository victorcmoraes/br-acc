MATCH (e) WHERE e.id = $id
RETURN e, labels(e) AS entity_labels
LIMIT 1
