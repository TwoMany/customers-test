// src/server.ts
import express from 'express';
import { ApolloServer, gql } from 'apollo-server-express';
import { MongoClient, ObjectId } from 'mongodb';
import { faker } from '@faker-js/faker';
import { createServer } from 'http';

const app = express();
const PORT = 3000;

require('dotenv').config();

const client = new MongoClient(process.env.DB_URI!);
client.connect();

const typeDefs = gql`
  type Customer {
    _id: String!
    firstName: String!
    lastName: String!
    email: String!
    address: Address!
    createdAt: String!
  }

  type Address {
    line1: String!
    line2: String!
    postcode: String!
    city: String!
    state: String!
    country: String!
  }

  type Query {
    customers: [Customer]
  }

  type Subscription {
    customerUpdated: Customer
  }
`;

const resolvers = {
  Query: {
    customers: async () => {
      const customers = await client.db('47Database').collection('customers').find().toArray();
      return customers;
    },
  },
  Subscription: {
    customerUpdated: {
      subscribe: (_: any, __: any, { pubsub }: any) => pubsub.asyncIterator(['CUSTOMER_UPDATED']),
    },
  },
};

const server = new ApolloServer({
  typeDefs,
  resolvers,
  context: ({ req }) => ({ req }),
});

async function startServer() {
  await server.start();

  server.applyMiddleware({ app });
  const httpServer = createServer(app);
  httpServer.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}${server.graphqlPath}`);
  })
  await customersFn();

  const changeStream = client.db('47Database').collection('customers').watch();

  changeStream.on('change', async (change) => {
    try {
      if (change.operationType === 'insert' || change.operationType === 'update') {
        const originalCustomer = change.fullDocument;
        const anonymizedCustomer = {
          _id: originalCustomer!._id.toHexString(),
          firstName: generateAnonymizedString(originalCustomer!.firstName),
          lastName: generateAnonymizedString(originalCustomer!.lastName),
          email: generateAnonymizedEmail(originalCustomer!.email),
          address: {
            line1: generateAnonymizedString(originalCustomer!.address.line1),
            line2: generateAnonymizedString(originalCustomer!.address.line2),
            postcode: generateAnonymizedString(originalCustomer!.address.postcode),
            city: generateAnonymizedString(originalCustomer!.address.city),
            state: generateAnonymizedString(originalCustomer!.address.state),
            country: generateAnonymizedString(originalCustomer!.address.country),
          },
          createdAt: originalCustomer!.createdAt,
        };

        console.log('Anonymized data:', anonymizedCustomer);

        await client.db('47Database').collection('customers_anonymised').insertOne(anonymizedCustomer);
      }
    } catch (error) {
      console.error('Error processing change:', error);
    }
  });
}

startServer().catch((error) => {
  console.error('Error starting server:', error);
});

async function customersFn() {
  setInterval(async () => {
    const originalCustomers = Array.from({ length: faker.number.int(10) + 1 }, () => ({
      _id: new ObjectId(),
      firstName: faker.person.firstName(),
      lastName: faker.person.lastName(),
      email: faker.internet.email(),
      address: {
        line1: faker.location.streetAddress(),
        line2: faker.location.secondaryAddress(),
        postcode: faker.location.zipCode(),
        city: faker.location.city(),
        state: faker.location.state(),
        country: faker.location.country(),
      },
      createdAt: new Date(),
    }));
    await client.db('47Database').collection('customers').insertMany(originalCustomers);
  }, 2000);
};

function generateAnonymizedString(originalString: string) {
  const characters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  const length = 8;
  let result = '';
  for (let i = 0; i < length; i++) {
    const randomIndex = Math.floor(Math.random() * characters.length);
    result += characters.charAt(randomIndex);
  }
  return result;
}

function generateAnonymizedEmail(originalEmail: string) {
  const [username, domain] = originalEmail.split('@');
  const anonymizedUsername = generateAnonymizedString(username);
  return `${anonymizedUsername}@${domain}.com`;
}