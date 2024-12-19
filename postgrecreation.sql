CREATE TABLE users (
    Name VARCHAR(100),
    Email VARCHAR(100),
    Age INT
);

-- Valid data
INSERT INTO users (Name, Email, Age) VALUES ('John Doe', 'john.doe@example.com', 25);
INSERT INTO users (Name, Email, Age) VALUES ('Jane Smith', 'jane.smith@example.com', 30);
INSERT INTO users (Name, Email, Age) VALUES ('Alice-Brown', 'alice.brown@example.com', 22);

-- Invalid email formats
INSERT INTO users (Name, Email, Age) VALUES ('Bob Green', 'bob.green.com', 28);
INSERT INTO users (Name, Email, Age) VALUES ('Cathy-Blue', 'cathy_blue@.com', 35);
INSERT INTO users (Name, Email, Age) VALUES ('David Black', 'david@black', 40);

-- Non-positive ages
INSERT INTO users (Name, Email, Age) VALUES ('Eve Red', 'eve.red@example.com', -5);
INSERT INTO users (Name, Email, Age) VALUES ('Frank White', 'frank.white@example.com', 0);

-- Names with hyphens
INSERT INTO users (Name, Email, Age) VALUES ('Grace-Yellow', 'grace.yellow@example.com', 45);
INSERT INTO users (Name, Email, Age) VALUES ('Henry-Orange', 'henry.orange@example.com', 50);

-- Combination of issues
INSERT INTO users (Name, Email, Age) VALUES ('Ivy-Purple', 'ivy.purple@.com', -10);
INSERT INTO users (Name, Email, Age) VALUES ('Jack-Grey', 'jack.grey@invalid', 0);
INSERT INTO users (Name, Email, Age) VALUES ('Kate-Gold', 'kate.gold', -15);

-- More valid data
INSERT INTO users (Name, Email, Age) VALUES ('Leo Blue', 'leo.blue@example.com', 19);
INSERT INTO users (Name, Email, Age) VALUES ('Mary Green', 'mary.green@example.com', 23);

-- Invalid data
INSERT INTO users (Name, Email, Age) VALUES ('Nick-Black', 'nick.black@', 0);
INSERT INTO users (Name, Email, Age) VALUES ('Olivia-Pink', 'olivia.pink.com', -3);
INSERT INTO users (Name, Email, Age) VALUES ('Peter-Brown', 'peter.brown@nowhere', -7);

-- Mixed valid and invalid
INSERT INTO users (Name, Email, Age) VALUES ('Quinn-Violet', 'quinn.violet@example.com', 33);
INSERT INTO users (Name, Email, Age) VALUES ('Ruth-White', 'ruth.white@.com', -2);
