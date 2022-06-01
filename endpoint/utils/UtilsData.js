const { Gateway, Wallets } = require('fabric-network');
const path = require('path');
const fs = require('fs');


async function addSieveLogs(userName, data) {
    return new Promise(async (resolve, reject) => {
        try {
            // load the network configuration
            const ccpPath = path.resolve(__dirname, '..', '..', '..', 'test-network', 'organizations', 'peerOrganizations', 'org1.example.com', 'connection-org1.json');
            let ccp = JSON.parse(fs.readFileSync(ccpPath, 'utf8'));
    
            // Create a new file system based wallet for managing identities.
            const walletPath = path.join(process.cwd(), 'wallet');
            const wallet = await Wallets.newFileSystemWallet(walletPath);
            // console.log(`Wallet path: ${walletPath}`);
    
            // Check to see if we've already enrolled the user.
            const identity = await wallet.get(userName);
            if (!identity) {
                return {"error": "User does not exit."};
            }
    
            // Create a new gateway for connecting to our peer node.
            const gateway = new Gateway();
            await gateway.connect(ccp, { wallet, identity: userName, discovery: { enabled: true, asLocalhost: true } });
            console.log('connected')
            // Get the network (channel) our contract is deployed to.
            const network = await gateway.getNetwork('mychannel');
            console.log('networked')
            // Get the contract from the network.
            const contract = network.getContract('fabcar');
            console.log('contracted')
            // Submit the specified transaction.
            await contract.submitTransaction('createSieveLogs', userName, JSON.stringify(data)).then(() => console.log('submitted'))
            .catch(error => reject({'error': `${error}`}));
            
            console.log('tried')
            // Disconnect from the gateway.
            gateway.disconnect()
            resolve({'success': 'Logs have been added.'})
    
        } catch (error) {
            reject({'error': `${error}`})
            
        }
    })
    
}


async function getSieveLogsByUser(userName, bookmark) {
    try {
        // load the network configuration
        const ccpPath = path.resolve(__dirname, '..', '..', '..', 'test-network', 'organizations', 'peerOrganizations', 'org1.example.com', 'connection-org1.json');
        const ccp = JSON.parse(fs.readFileSync(ccpPath, 'utf8'));

        // Create a new file system based wallet for managing identities.
        const walletPath = path.join(process.cwd(), 'wallet');
        const wallet = await Wallets.newFileSystemWallet(walletPath);
        // console.log(`Wallet path: ${walletPath}`);

        // Check to see if we've already enrolled the user.
        const identity = await wallet.get(userName);
        if (!identity) {
            return {"error": "User does not exit."};
        }

        // Create a new gateway for connecting to our peer node.
        const gateway = new Gateway();
        await gateway.connect(ccp, { wallet, identity: userName, discovery: { enabled: true, asLocalhost: true } });

            // Get the network (channel) our contract is deployed to.
        const network = await gateway.getNetwork('mychannel');

        // Get the contract from the network.
        const contract = network.getContract('fabcar');

        // Evaluate the specified transaction.
        // const result = await contract.evaluateTransaction(
        //     'querySieveLogsByUser',
        //     userName, "1000", bookmark
        //     );
        const result = await contract.evaluateTransaction('querySieveLogsByUser');
        
        // Disconnect from the gateway.
        await gateway.disconnect();

        return result;
        
    } catch (error) {
        return {"error": `Error: ${error}`};
    }
}



module.exports = { 
    getSieveLogsByUser,
    addSieveLogs
 };
