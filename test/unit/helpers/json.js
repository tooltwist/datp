const fs = require('fs');

const writeResponseToJson = (response, filePath) => {
   const responseStr = JSON.stringify(response, null, 2);
   fs.writeFileSync(filePath, responseStr);
};

module.exports = {
   writeResponseToJson,
};