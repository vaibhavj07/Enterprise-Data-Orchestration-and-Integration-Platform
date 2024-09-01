CREATE TABLE crm (
  CustomerID INT NOT NULL PRIMARY KEY,
  CustomerName VARCHAR(15),
  Email VARCHAR(30),
  Phone VARCHAR(15),
  SignupDate DATE
);

CREATE TABLE transactions (
  TransactionID INT NOT NULL PRIMARY KEY,
  CustomerID INT FOREIGN KEY REFERENCES crm(CustomerID),
  ProductID SMALLINT NOT NULL,
  Quantity SMALLINT,
  TransactionDate DATE,
  TransactionAmount DECIMAL
);

CREATE TABLE attribution (
  AttributionID INT NOT NULL PRIMARY KEY,
  CustomerID INT FOREIGN KEY REFERENCES crm(CustomerID),
  CampaignID SMALLINT NOT NULL,
  AttributionDate DATE,
  AttributionValue DECIMAL
);