import { BatchWriteItemCommand } from "@aws-sdk/client-dynamodb";


export async function batchWriteItems(
    batchWriteItems,
  full_tableName,
  dynamodbClient
) {
    

  const input = {
    RequestItems: {
        [full_tableName]: batchWriteItems,
    }
  };

  const command = new BatchWriteItemCommand(input);
  let response = await dynamodbClient.send(command);
  return response;
}
