/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
const path = require('path');
const fs = require('fs');

const RELATIVE_TEST_PATH = '../';
const RELATIVE_MODULES_PATH = '../../../modules';

const testsPath = path.join(__dirname, RELATIVE_TEST_PATH);
const modulesPath = path.join(__dirname, RELATIVE_MODULES_PATH);

// remove files (prob with extensions .) and keep folders only
const testFolders = fs.readdirSync(testsPath)
   .filter(folder => folder !== 'helpers' && !folder.includes('.'));
const moduleFolders = fs.readdirSync(modulesPath).filter(folder => !folder.includes('.'));

const isController = fileName => {
   return fileName.startsWith('get_') ||
      fileName.startsWith('post_') ||
      fileName.startsWith('put_') ||
      fileName.startsWith('delete_') ||
      fileName.startsWith('patch_');
};

// this assumes that module folder names are === to test folder names
// and that test names === module names
const moduleReports = moduleFolders
   .filter(moduleFolder => testFolders.includes(moduleFolder))
   .map(moduleFolder => {
      const specificModuleRelativePath = `${RELATIVE_MODULES_PATH}/${moduleFolder}/`;
      const specificModulePath = path.join(__dirname, specificModuleRelativePath);
      const moduleFiles = fs.readdirSync(specificModulePath)
         .map(fileName => fileName.toLowerCase())
         .filter(
            fileName => isController(fileName) &&
               !fileName.endsWith('-obsolete')
         );

      const specificTestsRelativePath = `${RELATIVE_TEST_PATH}/${moduleFolder}/`;
      const specificTestsPath = path.join(__dirname, specificTestsRelativePath);
      const testFiles = fs.readdirSync(specificTestsPath)
         .map(fileName => fileName.toLowerCase())
         .filter(fileName => fileName.startsWith('ava-'))
         .map(fileName => fileName.replace('ava-', ''));

      const missingTestsOnSpecificModule = moduleFiles.filter(moduleFileName => !testFiles.includes(moduleFileName));
      const totalModuleFiles = moduleFiles.length;

      // We don't want to get a weird value with the testing percentage for those addtl tests
      // so total test files === those with matching file names on their module folder equivalent
      const matchingTestsAndModules = testFiles.filter(testFile => moduleFiles.includes(testFile));
      const totalTestFiles = matchingTestsAndModules.length;

      const testCoverageInPercent = totalModuleFiles === 0 ? 100 :
         Math.round((totalTestFiles / totalModuleFiles) * 100);

      return {
         moduleFolder,
         missingTestsOnSpecificModule,
         totalModuleFiles,
         totalTestFiles,
         testCoverageInPercent,
      }
   });

moduleReports.forEach(moduleReport => {
   const {
      moduleFolder,
      missingTestsOnSpecificModule,
      totalModuleFiles,
      totalTestFiles,
      testCoverageInPercent,
   } = moduleReport;

   console.log(`Module: ${moduleFolder}`);
   console.log('Missing tests: ', missingTestsOnSpecificModule);
   console.log(`Test coverage: ${testCoverageInPercent}%`);
   console.log(`Total files in module ${totalModuleFiles}`);
   console.log(`Total tests for module ${totalTestFiles}`);
   console.log(`Total missing tests for module: ${missingTestsOnSpecificModule.length}`)
   console.log('-----------------------------------------');
});

const totalNumberOfModules = moduleReports.reduce((a, { totalModuleFiles }) => a + totalModuleFiles, 0);
const totalNumberOfTests = moduleReports.reduce((a, { totalTestFiles }) => a + totalTestFiles, 0);
const approxTotalTestCoverage = Math.round((totalNumberOfTests / totalNumberOfModules) * 100);
const modulesWithNoTests = moduleFolders.filter(moduleFolder => !testFolders.includes(moduleFolder));

console.log('\t\tSUMMARY')
console.log('-----------------------------------------');
console.log(`TOTAL NUMBER OF MODULES: ${totalNumberOfModules}`);
console.log(`TOTAL NUMBER OF TESTS: ${totalNumberOfTests}`);
console.log(`APPROX. MODULES COVERAGE: ${approxTotalTestCoverage}%`);
console.log('MODULES WITH NO TESTS: ', modulesWithNoTests);
