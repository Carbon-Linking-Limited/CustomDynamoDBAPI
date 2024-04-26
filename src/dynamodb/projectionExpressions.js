export function generateProjectionExpressions(projectionFields) {
  let projectionExpressions = "";
  let _ExpressionAttributeNames = {};
  let _ExpressionData = {};
  try {
    validateProjectionFields(projectionFields);
  } catch (err) {
    console.log(err);
    return;
  }
  try {
    for (let projectionField of projectionFields) {
      let expressionName = `#${projectionField}`;
      let attributeName = `${projectionField}`;
      _ExpressionAttributeNames[expressionName] = attributeName;
      projectionExpressions = projectionExpressions.concat(expressionName, ",");
    }
  } catch (err) {
    console.log(err);
    return;
  }

  if (projectionExpressions !== "") {
    //removing the trailing "," from expression.
    projectionExpressions = projectionExpressions.slice(0, -1);
  }
  _ExpressionData = {
    expressionAttributeNames: _ExpressionAttributeNames,
    projectionExpressions: projectionExpressions,
  };
  return _ExpressionData;
}

function validateProjectionFields(projectionFields){
    if (!Array.isArray(projectionFields)){
        throw new TypeError (`Incorrect Type for projectionFields: ${typeof projectionFields}. ProjectionFields must be of type [String!].`)
    }
    if (typeof projectionFields[0] !== "string"){
        throw new TypeError (`Incorrect Type for projectionFields: ${typeof projectionFields[0]}. ProjectionFields must be of type [String!].`)
    }
}