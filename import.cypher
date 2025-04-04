CREATE INDEX FOR (p:Person) ON (p.raw_name);
CREATE INDEX FOR (p:Person) ON (p.full_name);

// Person node creation
:auto LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/andre91a/estfiu-dataset/refs/heads/main/fixed/person_fixed.csv' AS row
WITH row
CALL {
    WITH row
    MERGE (p:Person {full_name: row.full_name})
    ON CREATE SET p.first_name = row.first_name, 
                  p.last_name = row.last_name,
                  p.raw_name = row.first_name + " " + row.last_name
} IN TRANSACTIONS OF 10000 ROWS;


// Company node creation
CREATE INDEX FOR (c:Company) ON (c.company_name);

:auto LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/andre91a/estfiu-dataset/refs/heads/main/fixed/company_fixed.csv' AS row
WITH row
CALL {
    WITH row
    MERGE (c:Company {company_id: row.company_id})
    ON CREATE SET c.company_name = row.company_name,
                  c.status = CASE WHEN row.status <> '' THEN row.status ELSE NULL END,
                  c.start_date = CASE WHEN row.start_date <> '' THEN row.start_date ELSE NULL END,
                  c.company_id_generic = CASE WHEN row.company_id_generic <> '' THEN row.company_id_generic ELSE NULL END
} IN TRANSACTIONS OF 10000 ROWS;

//IS_SHAREHOLDER relationship creation (from Person to Company)
:auto LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/andre91a/estfiu-dataset/refs/heads/main/fixed/shareholding_relation_person_fixed.csv' AS row
WITH row
CALL {
    WITH row
    MATCH (p:Person {raw_name: row.first_name + " " + row.last_name})
    WHERE p.first_name IS NOT NULL AND p.last_name IS NOT NULL
    MATCH (c:Company {company_id: row.company_id})
    MERGE (p)-[r:IS_SHAREHOLDER]->(c)
    ON CREATE SET r.percentage_of_participation = CASE WHEN row.percentage_of_participation <> '' THEN toFloat(row.percentage_of_participation) ELSE NULL END,
                  r.size_of_the_participation = CASE WHEN row.the_size_of_the_participation <> '' THEN toFloat(row.the_size_of_the_participation) ELSE NULL END,
                  r.currency_of_the_stake = CASE WHEN row.the_currency_of_the_stake <> '' THEN row.the_currency_of_the_stake ELSE NULL END,
                  r.type_of_ownership_as_a_text = CASE WHEN row.Type_of_ownership_as_a_text <> '' THEN row.Type_of_ownership_as_a_text ELSE NULL END,
                  r.start_date = CASE WHEN row.start_date <> '' THEN row.start_date ELSE NULL END,
                  r.species_of_the_owner_of_the_owner = CASE WHEN row.the_species_of_the_owner_of_the_owner <> '' THEN row.the_species_of_the_owner_of_the_owner ELSE NULL END,
                  r.type_of_shareholding = CASE WHEN row.type_of_shareholding <> '' THEN row.type_of_shareholding ELSE NULL END,
                  r.role_of_shareholder = CASE WHEN row.role_of_shareholder <> '' THEN row.role_of_shareholder ELSE NULL END
} IN TRANSACTIONS OF 10000 ROWS;

//TODO
//IS_SHAREHOLDER relationship creation (from Company to Company)
:auto LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/andre91a/estfiu-dataset/refs/heads/main/fixed/shareholding_relation_company_fixed.csv' AS row
WITH row
CALL {
    WITH row
    MATCH (c1:Company {company_id: row.company_id})
    MATCH (c2:Company {company_name: row.company_name}) 
    MERGE (c1)-[r:IS_SHAREHOLDER]->(c2)
    ON CREATE SET r.percentage_of_participation = CASE WHEN row.percentage_of_participation <> '' THEN toFloat(row.percentage_of_participation) ELSE NULL END,
                  r.size_of_the_participation = CASE WHEN row.the_size_of_the_participation <> '' THEN toFloat(row.the_size_of_the_participation) ELSE NULL END,
                  r.currency_of_the_stake = CASE WHEN row.the_currency_of_the_stake <> '' THEN row.the_currency_of_the_stake ELSE NULL END,
                  r.type_of_ownership_as_a_text = CASE WHEN row.Type_of_ownership_as_a_text <> '' THEN row.Type_of_ownership_as_a_text ELSE NULL END,
                  r.start_date = CASE WHEN row.start_date <> '' THEN row.start_date ELSE NULL END,
                  r.species_of_the_owner_of_the_owner = CASE WHEN row.the_species_of_the_owner_of_the_owner <> '' THEN row.the_species_of_the_owner_of_the_owner ELSE NULL END,
                  r.type_of_shareholding = CASE WHEN row.type_of_shareholding <> '' THEN row.type_of_shareholding ELSE NULL END,
                  r.role_of_shareholder = CASE WHEN row.role_of_shareholder <> '' THEN row.role_of_shareholder ELSE NULL END
} IN TRANSACTIONS OF 10000 ROWS;

//HAS_ADDRESS relationship creation (from Person to Address)
:auto LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/andre91a/estfiu-dataset/refs/heads/main/fixed/person_address_fixed.csv' AS row
WITH row
WHERE row.address IS NOT NULL 
CALL {
    WITH row
    MATCH (p:Person {raw_name: row.first_name + " " + row.last_name})
    WHERE p.first_name IS NOT NULL AND p.last_name IS NOT NULL
    MERGE (a:Address {address: row.address})    
    ON CREATE SET a.country = CASE WHEN row.country <> '' THEN row.country ELSE NULL END
    
    MERGE (p)-[r:HAS_ADDRESS]->(a)  
    ON CREATE SET r.source_of_address = CASE WHEN row.source_of_address <> '' THEN row.source_of_address ELSE NULL END
} IN TRANSACTIONS OF 10000 ROWS;

//HAS_ADDRESS relationship creation (from Company to Address)
:auto LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/andre91a/estfiu-dataset/refs/heads/main/fixed/company_address_fixed.csv' AS row
WITH row
WHERE row.address IS NOT NULL
CALL {  
    WITH row
    MATCH (c:Company {company_id: row.company_id})  
    MERGE (a:Address {address: row.address})      
    ON CREATE SET a.country = CASE WHEN row.country <> '' THEN row.country ELSE NULL END

    MERGE (c)-[r:HAS_ADDRESS]->(a)  
    ON CREATE SET r.source_of_address = CASE WHEN row.source_of_address <> '' THEN row.source_of_address ELSE NULL END
} IN TRANSACTIONS OF 10000 ROWS;

//IS_BENEFICIARY relationship creation (from Person to Company)
:auto LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/andre91a/estfiu-dataset/refs/heads/main/fixed/beneficiar_fixed.csv' AS row
WITH row
WHERE row.first_name IS NOT NULL AND row.last_name IS NOT NULL
CALL {  
    WITH row
    MATCH (p:Person {raw_name: row.first_name + ' ' + row.last_name})
    MATCH (c:Company {company_id: row.company_id})
    MERGE (p)-[r:IS_BENEFICIARY]->(c)
    ON CREATE SET r.start_date = CASE WHEN row.start_date <> '' THEN row.start_date ELSE NULL END,
                  r.control_type = CASE WHEN row.the_way_in_which_the_control_is_done_as_text <> '' THEN row.the_way_in_which_the_control_is_done_as_text ELSE NULL END,
                  r.disconnection_notice_submitted = CASE WHEN row.Disconnection_Notice_submitted <> '' THEN row.Disconnection_Notice_submitted ELSE NULL END,
                  r.role_of_ben = CASE WHEN row.role_of_ben <> '' THEN row.role_of_ben ELSE NULL END
} IN TRANSACTIONS OF 10000 ROWS;

//Add Role to Person
:auto LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/andre91a/estfiu-dataset/refs/heads/main/fixed/person_role_fixed.csv' AS row
WITH row
WHERE row.full_name IS NOT NULL
CALL {
    WITH row
    MATCH (p:Person {raw_name: row.first_name + " " + row.last_name})
    SET p.role = CASE 
        WHEN p.role IS NULL THEN [row.role_within_data]
        ELSE p.role + [row.role_within_data]
    END
} IN TRANSACTIONS OF 10000 ROWS;


//Remove raw_name property from Person nodes
:auto CALL {
    MATCH (a:Person)
    REMOVE a.raw_name
} IN TRANSACTIONS OF 10000 ROWS;