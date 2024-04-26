

export function randNum(){
    return Math.floor(Math.random() * 1000)
}
export function mapAttributTypeToAWSContentShorthand(attributeType) {
    //unsupprted Content type: Binary Set, Binary,
    switch (attributeType.toLowerCase()) {
      case "string":
        return "S";
      case "stringset":
        return "SS";
      case "bool":
        return "BOOL";
      case "list":
      case "array":
        return "L";
      case "map":
      case "object":
        return "M";
      case "number":
      case "int":
      case "integer":
      case "float":
      case "double":
        return "N";
      case "numberset":
        return "NS";
      case "null":
        return "NULL";
      default:
        return "not_supported";
    }
  }

 