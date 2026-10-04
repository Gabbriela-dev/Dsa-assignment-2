import ballerina/http;

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

map<Customer> customers = {};
map<Address> addresses = {};

int customerCounter = 1;
int addressCounter = 1;

service /customers on new http:Listener(8081) {

    resource function get health() returns string {
        return "Customer Service is running";
    }

    // CREATE CUSTOMER
    resource function post .(@http:Payload NewCustomer newCustomer)
            returns Customer {

        string customerId = string `CUS-${customerCounter}`;
        customerCounter += 1;

        Customer createdCustomer = {
            customerId: customerId,
            name: newCustomer.name,
            email: newCustomer.email,
            phone: newCustomer.phone
        };

        customers[customerId] = createdCustomer;

        return createdCustomer;
    }

    // GET CUSTOMER
    resource function get [string customerId]()
            returns Customer|http:NotFound {

        Customer? foundCustomer = customers[customerId];

        if foundCustomer is Customer {
            return foundCustomer;
        }

        return <http:NotFound>{
            body: {
                message: "Customer not found"
            }
        };
    }

    // UPDATE CUSTOMER
    resource function put [string customerId](
            @http:Payload NewCustomer updatedCustomer)
            returns Customer|http:NotFound {

        if !customers.hasKey(customerId) {
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

        customers[customerId] = customer;

        return customer;
    }

    // DELETE CUSTOMER
    resource function delete [string customerId]()
            returns string|http:NotFound {

        if !customers.hasKey(customerId) {
            return <http:NotFound>{
                body: {
                    message: "Customer not found"
                }
            };
        }

        _ = customers.remove(customerId);

        return "Customer deleted successfully";
    }

    // ADD DELIVERY ADDRESS
    resource function post [string customerId]/addresses(
            @http:Payload NewAddress newAddress)
            returns Address|http:NotFound {

        if !customers.hasKey(customerId) {
            return <http:NotFound>{
                body: {
                    message: "Customer not found"
                }
            };
        }

        string addressId = string `ADDR-${addressCounter}`;
        addressCounter += 1;

        Address createdAddress = {
            addressId: addressId,
            customerId: customerId,
            label: newAddress.label,
            address: newAddress.address
        };

        addresses[addressId] = createdAddress;

        return createdAddress;
    }

    // GET CUSTOMER DELIVERY ADDRESSES
    resource function get [string customerId]/addresses()
            returns Address[]|http:NotFound {

        if !customers.hasKey(customerId) {
            return <http:NotFound>{
                body: {
                    message: "Customer not found"
                }
            };
        }

        Address[] customerAddresses = [];

        foreach Address customerAddress in addresses {
            if customerAddress.customerId == customerId {
                customerAddresses.push(customerAddress);
            }
        }

        return customerAddresses;
    }
}
