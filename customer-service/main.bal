import ballerina/http;
import ballerina/sql;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;


// DATABASE CONFIGURATION


configurable string dbHost = ?;
configurable int dbPort = ?;
configurable string dbUser = ?;
configurable string dbPassword = ?;
configurable string dbName = ?;

final mysql:Client dbClient = check new (
    host = dbHost,
    user = dbUser,
    password = dbPassword,
    database = dbName,
    port = dbPort
);


// RECORD TYPES


type Customer record {|
    string customerId;
    string name;
    string email;
    string phone;
|};

type NewCustomer record {|
    string name;
    string email;
    string phone;
|};

type Address record {|
    string addressId;
    string customerId;
    string label;
    string address;
|};

type NewAddress record {|
    string label;
    string address;
|};

type CounterResult record {|
    int nextId;
|};


// ID COUNTERS


int customerCounter = 1;
int addressCounter = 1;


// INITIALISE COUNTERS


function initialiseCounters() returns error? {

    CounterResult customerResult = check dbClient->queryRow(
        `SELECT CAST(
            COALESCE(
                MAX(CAST(SUBSTRING(customer_id, 5) AS UNSIGNED)),
                0
            ) + 1
            AS SIGNED
         ) AS nextId
         FROM customers`,
        CounterResult
    );

    customerCounter = customerResult.nextId;

    CounterResult addressResult = check dbClient->queryRow(
        `SELECT CAST(
            COALESCE(
                MAX(CAST(SUBSTRING(address_id, 6) AS UNSIGNED)),
                0
            ) + 1
            AS SIGNED
         ) AS nextId
         FROM addresses`,
        CounterResult
    );

    addressCounter = addressResult.nextId;
}


// STARTUP


public function main() returns error? {
    check initialiseCounters();
}


// CUSTOMER SERVICE


service /customers on new http:Listener(8081) {

    
    // HEALTH CHECK
   

    resource function get health() returns string {
        return "Customer Service is running";
    }

   
    // CREATE CUSTOMER
    

    resource function post .(@http:Payload NewCustomer newCustomer)
            returns Customer|http:InternalServerError {

        string customerId = string `CUS-${customerCounter}`;
        customerCounter += 1;

        sql:ExecutionResult|error result = dbClient->execute(
            `INSERT INTO customers
            (customer_id, name, email, phone)
            VALUES
            (${customerId}, ${newCustomer.name},
             ${newCustomer.email}, ${newCustomer.phone})`
        );

        if result is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to create customer"
                }
            };
        }

        Customer createdCustomer = {
            customerId: customerId,
            name: newCustomer.name,
            email: newCustomer.email,
            phone: newCustomer.phone
        };

        return createdCustomer;
    }

        // GET CUSTOMER
   

    resource function get [string customerId]()
            returns Customer|http:NotFound|http:InternalServerError {

        Customer|error foundCustomer = dbClient->queryRow(
            `SELECT
                customer_id AS customerId,
                name,
                email,
                phone
             FROM customers
             WHERE customer_id = ${customerId}`,
            Customer
        );

        if foundCustomer is Customer {
            return foundCustomer;
        }

        if foundCustomer is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Customer not found"
                }
            };
        }

        return <http:InternalServerError>{
            body: {
                message: "Failed to retrieve customer"
            }
        };
    }

    // UPDATE CUSTOMER
  

    resource function put [string customerId](
            @http:Payload NewCustomer updatedCustomer)
            returns Customer|http:NotFound|http:InternalServerError {

        sql:ExecutionResult|error result = dbClient->execute(
            `UPDATE customers
             SET name = ${updatedCustomer.name},
                 email = ${updatedCustomer.email},
                 phone = ${updatedCustomer.phone}
             WHERE customer_id = ${customerId}`
        );

        if result is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to update customer"
                }
            };
        }

        if result.affectedRowCount == 0 {
            return <http:NotFound>{
                body: {
                    message: "Customer not found"
                }
            };
        }

        Customer customer = {
            customerId: customerId,
            name: updatedCustomer.name,
            email: updatedCustomer.email,
            phone: updatedCustomer.phone
        };

        return customer;
    }

   
    // DELETE CUSTOMER
   

    resource function delete [string customerId]()
            returns string|http:NotFound|http:InternalServerError {

        sql:ExecutionResult|error result = dbClient->execute(
            `DELETE FROM customers
             WHERE customer_id = ${customerId}`
        );

        if result is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to delete customer"
                }
            };
        }

        if result.affectedRowCount == 0 {
            return <http:NotFound>{
                body: {
                    message: "Customer not found"
                }
            };
        }

        return "Customer deleted successfully";
    }

    
    // ADD DELIVERY ADDRESS
    

    resource function post [string customerId]/addresses(
            @http:Payload NewAddress newAddress)
            returns Address|http:NotFound|http:InternalServerError {

        Customer|error foundCustomer = dbClient->queryRow(
            `SELECT
                customer_id AS customerId,
                name,
                email,
                phone
             FROM customers
             WHERE customer_id = ${customerId}`,
            Customer
        );

        if foundCustomer is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Customer not found"
                }
            };
        }

        if foundCustomer is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve customer"
                }
            };
        }

        string addressId = string `ADDR-${addressCounter}`;
        addressCounter += 1;

        sql:ExecutionResult|error result = dbClient->execute(
            `INSERT INTO addresses
            (address_id, customer_id, label, address)
            VALUES
            (${addressId}, ${customerId},
             ${newAddress.label}, ${newAddress.address})`
        );

        if result is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to add delivery address"
                }
            };
        }

        Address createdAddress = {
            addressId: addressId,
            customerId: customerId,
            label: newAddress.label,
            address: newAddress.address
        };

        return createdAddress;
    }

   
    // GET DELIVERY ADDRESSES
  

    resource function get [string customerId]/addresses()
            returns Address[]|http:NotFound|http:InternalServerError {

        Customer|error foundCustomer = dbClient->queryRow(
            `SELECT
                customer_id AS customerId,
                name,
                email,
                phone
             FROM customers
             WHERE customer_id = ${customerId}`,
            Customer
        );

        if foundCustomer is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Customer not found"
                }
            };
        }

        if foundCustomer is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve customer"
                }
            };
        }

        stream<Address, sql:Error?> addressStream = dbClient->query(
            `SELECT
                address_id AS addressId,
                customer_id AS customerId,
                label,
                address
             FROM addresses
             WHERE customer_id = ${customerId}`,
            Address
        );

        Address[] customerAddresses = [];

        error? streamError = addressStream.forEach(
            function(Address customerAddress) {
                customerAddresses.push(customerAddress);
            }
        );

        if streamError is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve addresses"
                }
            };
        }

        return customerAddresses;
    }
}