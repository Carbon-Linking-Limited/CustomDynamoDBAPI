import { TransactWriteItemsCommand } from "@aws-sdk/client-dynamodb";

export async function transactWriteItems(transactItems, dynamodbClient){
    const transactWriteParams = {
      TransactItems: [...transactItems],
    };
    const command = new TransactWriteItemsCommand(transactWriteParams);
    const response = await dynamodbClient.send(command);
    return response;
   }
  