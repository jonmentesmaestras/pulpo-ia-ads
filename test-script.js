// filepath: c:\Users\LENOVO\Desktop\pulpoAi\ads\test-script.js
const { fetchActiveDetails } = require('./app.js');

// A list of ad IDs to test.
// Includes a mix of potentially active and inactive IDs.
const testIds = [
    '1835465637059354',  
    '1945663559626909',
    '25163621736599964',
    '827311800218763',
    '829571056670963'  
];

// An async function to run the test and log the output.
async function runTest() {
    console.log('Calling fetchActiveDetails with IDs:', testIds);

    try {
        // Call the function.
        const activeStatusMap = await fetchActiveDetails(testIds);

        // Log the result to the console.
        console.log('\n--- Result from fetchActiveDetails ---');
        console.log(activeStatusMap);
        console.log('--------------------------------------\n');

    } catch (error) {
        console.error('An error occurred while running the test:', error);
    }
}

// Execute the test function.
runTest();