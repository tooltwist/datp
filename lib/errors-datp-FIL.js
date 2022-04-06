export default {
  "scope": "datp",
  "lang": "FIL",
  "description": "Error codes for DATP",
  "errors": [

    {
      "code": "datp-400-0001",
      "name": "FIELD_IS_INVALID",
      "httpStatus": 400,
      "message": "Di-wasto ang field na '{{field}}'."
    },
    
    {
      "code": "datp-400-0002",
      "name": "FIELD_IS_REQUIRED",
      "httpStatus": 400,
      "message": "Kinakailangan ang field na '{{field}}'."
    },
    
    {
      "code": "datp-400-0003",
      "name": "FILE_IS_INVALID",
      "httpStatus": 400,
      "message": "Ang file ay dapat na isang maximum na {{field}}MB lamang."
    },
    
    {
      "code": "datp-400-0004",
      "name": "EXTERNAL_ID_ALREADY_EXIST",
      "httpStatus": 400,
      "message": "Umiiral na ang External Id."
    },

    {
      "code": "datp-400-0002",
      "name": "FIELD_IS_UNKNOWN",
      "httpStatus": 400,
      "message": "Ang hindi kilalang field na '{{field}}' ay hindi dapat nasa kahilingang ito."
    },
    
    {
      "code": "datp-400-0003",
      "name": "FIELD_ENUM_IS_INVALID",
      "httpStatus": "400",
      "message": "Di-wasto ang field na '{{field}}'. Mga katanggap-tanggap na halaga: {{values}}",
    },

    {
      "code": "datp-404-0001",
      "name": "FIELD_NOT_FOUND",
      "httpStatus": 404,
      "message": "Hindi nahanap ang field na '{{field}}'."
    },
    
    // {
    //   "name": "UNAUTHORIZED_INVALID_TENANT_ID",
    //   "httpStatus": 401,
    //   "code": "401-0001",
    //   "message": "Hindi awtorisado: Nawawala o di-wastong access key ng nangungupahan."
    // },
    
    
    {
      "code": "datp-500-0001",
      "name": "GENERIC_ERROR",
      "httpStatus": 500,
      "message": "May nangyaring mali. Mangyaring makipag-ugnayan sa iyong administrator."
    },
    
    // {
    //   "name": "DOCUMENT_UPLOAD_ERROR",
    //   "httpStatus": 500,
    //   "code": "500-0002",
    //   "message": "Error sa pag-upload ng dokumento."
    // }
  ]
}
