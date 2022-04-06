export default {
  "scope": "datp",
  "lang": "EN",
  "description": "Error codes for DATP",
  "errors": [

    {
      "code": "datp-400-0001",
      "name": "FIELD_IS_INVALID",
      "httpStatus": 400,
      "message": "Field '{{field}}' is invalid."
    },
    
    {
      "code": "datp-400-0002",
      "name": "FIELD_IS_REQUIRED",
      "httpStatus": 400,
      "message": "Field '{{field}}' is required."
    },
    
    {
      "code": "datp-400-0003",
      "name": "FILE_IS_INVALID",
      "httpStatus": 400,
      "message": "File must be a maximum of {{field}}MB only."
    },
    
    {
      "code": "datp-400-0004",
      "name": "EXTERNAL_ID_ALREADY_EXIST",
      "httpStatus": 400,
      "message": "External Id already exists."
    },

    {
      "code": "datp-400-0002",
      "name": "FIELD_IS_UNKNOWN",
      "httpStatus": 400,
      "message": "Unknown field '{{field}}' should not be in this request."
    },
    // {
    //   "code": "datp-400-9999",
    //   "name": "ERROR_NO_DONUTSZ",
    //   "httpStatus": "404",
    //   "message": "Big problem, we have run out of {{size}} {{flavor}} donuts!",
    // },
    {
      "code": "datp-400-0003",
      "name": "FIELD_ENUM_IS_INVALID",
      "httpStatus": "400",
      "message": "Field '{{field}}' is invalid. Acceptable values: {{values}}",
    },
  
    // {
    //   "code": "datp-400-0003",
    //   "name": "AMT_CURRENCY_IS_REQUIRED",
    //   "httpStatus": "400",
    //   "message": "{{field}}.currency is required",
    // },

    // {
    //     "code": "datp-400-0004",
    //     "name": "AMT_UNSCALED_AMOUNT_IS_REQUIRED",
    //     "httpStatus": "400",
    //     "message": "{{field}}.unscaledAmount is required",
    // },

    // {
    //     "code": "datp-400-0005",
    //     "name": "AMT_SCALE_IS_REQUIRED",
    //     "httpStatus": "400",
    //     "message": "{{field}}.scale is required",
    // },
    
    {
      "code": "datp-404-0001",
      "name": "FIELD_NOT_FOUND",
      "httpStatus": 404,
      "message": "{{field}} not found."
    },
    
    // {
    //   "name": "UNAUTHORIZED_INVALID_TENANT_ID",
    //   "httpStatus": 401,
    //   "code": "datp-401-0001",
    //   "message": "Unauthorized: Missing or invalid tenant access key."
    // },
    
    
    {
      "name": "GENERIC_ERROR",
      "httpStatus": 500,
      "code": "datp-500-0001",
      "message": "Something went wrong. Please contact your administrator."
    },
    
    // {
    //   "name": "DOCUMENT_UPLOAD_ERROR",
    //   "httpStatus": 500,
    //   "code": "datp-500-0002",
    //   "message": "Error uploading document."
    // }
  ]
}
