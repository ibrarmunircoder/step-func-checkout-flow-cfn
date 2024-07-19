const {
  QueryCommand,
  DynamoDBClient,
  GetItemCommand,
  UpdateItemCommand,
} = require('@aws-sdk/client-dynamodb');
const { marshall } = require('@aws-sdk/util-dynamodb');
const { NodeHttpHandler } = require('@smithy/node-http-handler');

const ddbClient = new DynamoDBClient({
  region: 'us-east-1',
  maxAttempts: 3,
  requestHandler: new NodeHttpHandler({
    requestTimeout: 5000,
  }),
});

const isBookAvailable = (book, quantity) => {
  return book.quantity - quantity > 0;
};

const deductPoints = async (userId) => {
  const command = new UpdateItemCommand({
    TableName: 'userTable',
    Key: marshall({ userId: userId }),
    UpdateExpression: 'set points = :zero',
    ExpressionAttributeValues: marshall({
      ':zero': 0,
    }),
  });
  await ddbClient.send(command);
};

const updateBookQuantity = async (bookId, orderQuantity) => {
  console.log('bookId: ', bookId);
  console.log('orderQuantity: ', orderQuantity);
  const command = new UpdateItemCommand({
    TableName: 'userTable',
    Key: marshall({ bookId: bookId }),
    UpdateExpression: 'SET quantity = quantity - :orderQuantity',
    ExpressionAttributeValues: marshall({
      ':orderQuantity': orderQuantity,
    }),
  });

  await ddbClient.send(command);
};

exports.checkInventory = async ({ bookId, quantity }) => {
  try {
    const command = new QueryCommand({
      TableName: 'bookTable',
      KeyConditionExpression: 'bookId = :bookId',
      ExpressionAttributeValues: marshall({
        ':bookId': bookId,
      }),
    });
    const result = await ddbClient.send(command);
    let book = result.Items[0];

    if (isBookAvailable(book, quantity)) {
      return book;
    } else {
      let bookOutOfStockError = new Error('The book is out of stock');
      bookOutOfStockError.name = 'BookOutOfStock';
      throw bookOutOfStockError;
    }
  } catch (e) {
    if (e.name === 'BookOutOfStock') {
      throw e;
    } else {
      let bookNotFoundError = new Error(e);
      bookNotFoundError.name = 'BookNotFound';
      throw bookNotFoundError;
    }
  }
};
exports.calculateTotal = async (event) => {
  console.log('book: ', book);
  let total = book.price * quantity;
  return { total };
};
exports.redeemPoints = async ({ userId, total }) => {
  console.log('userId: ', userId);
  const orderTotal = total.total;
  console.log('orderTotal:', orderTotal);
  try {
    const command = new GetItemCommand({
      TableName: 'userTable',
      Key: marshall({
        userId: userId,
      }),
    });
    let result = await ddbClient.send(command);
    let user = result.Item;
    console.log('user: ', user);
    const points = user.points;
    console.log('points: ', points);
    if (orderTotal > points) {
      await deductPoints(userId);
      orderTotal = orderTotal - points;
      return { total: orderTotal, points };
    } else {
      throw new Error('Order total is less than redeem points');
    }
  } catch (e) {
    throw new Error(e);
  }
};
exports.billCustomer = async (event) => {
  console.log(params);
  // throw 'Error in billing'
  /* Bill the customer e.g. Using Stripe token from the paramerters */
  return 'Successfully Billed';
};
exports.restoreQuantity = async (event) => {
  const command = new UpdateItemCommand({
    TableName: 'userTable',
    Key: marshall({ bookId: bookId }),
    UpdateExpression: 'SET quantity = quantity + :orderQuantity',
    ExpressionAttributeValues: marshall({
      ':orderQuantity': orderQuantity,
    }),
  });

  await ddbClient.send(command);

  return 'Quantity restored';
};
exports.restoreRedeemPoints = async ({ userId, total }) => {
  try {
    if (total.points) {
      const command = new UpdateItemCommand({
        TableName: 'userTable',
        Key: marshall({ userId: userId }),
        UpdateExpression: 'set points = :zero',
        ExpressionAttributeValues: marshall({
          ':zero': total.points,
        }),
      });
      await ddbClient.send(command);
    }
  } catch (e) {
    throw new Error(e);
  }
};

exports.sqsWorker = async (event) => {
  try {
    console.log(JSON.stringify(event));
    let record = event.Records[0];
    var body = JSON.parse(record.body);
    /** Find a courier and attach courier information to the order */
    let courier = '<courier email>';

    // update book quantity
    await updateBookQuantity(body.Input.bookId, body.Input.quantity);

    // throw "Something wrong with Courier API";

    // Attach curier information to the order
    await StepFunction.sendTaskSuccess({
      output: JSON.stringify({ courier }),
      taskToken: body.Token,
    }).promise();
  } catch (e) {
    console.log('===== You got an Error =====');
    console.log(e);
    await StepFunction.sendTaskFailure({
      error: 'NoCourierAvailable',
      cause: 'No couriers are available',
      taskToken: body.Token,
    }).promise();
  }
};
