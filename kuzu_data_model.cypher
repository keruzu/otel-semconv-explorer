CREATE NODE TABLE IF NOT EXISTS AttributeGroup(
    id STRING,
    display_name STRING,
    brief STRING,
    PRIMARY KEY (id)
);
CREATE NODE TABLE IF NOT EXISTS Attribute(
    id STRING,
    stability STRING,
    brief STRING,
    examples STRING,
    note STRING,
    PRIMARY KEY (id)
);
CREATE NODE TABLE IF NOT EXISTS Span(
    id STRING,
    span_kind STRING,
    stability STRING,
    brief STRING,
    note STRING,
    example STRING DEFAULT "",
    PRIMARY KEY (id)
);
CREATE NODE TABLE IF NOT EXISTS Entity(
    id STRING,
    stability STRING,
    brief STRING,
    name STRING,
    PRIMARY KEY (id)
);
CREATE NODE TABLE IF NOT EXISTS Event(
    id STRING,
    stability STRING,
    brief STRING,
    name STRING,
    example STRING DEFAULT "",
    PRIMARY KEY (id)
);
CREATE NODE TABLE IF NOT EXISTS Metric(
    id STRING,
    stability STRING,
    brief STRING,
    metric_name STRING,
    instrument STRING,
    unit STRING DEFAULT "",
    example STRING DEFAULT "",
    PRIMARY KEY (id)
);

CREATE REL TABLE IF NOT EXISTS HasAttribute(
  FROM AttributeGroup TO Attribute,
  FROM Metric TO Attribute,
  FROM Entity TO Attribute,
  FROM Span TO Attribute,
  FROM Event TO Attribute
);
CREATE REL TABLE IF NOT EXISTS HasEvent(
  FROM Span TO Event
);

INSTALL json;
LOAD json;
