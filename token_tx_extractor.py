import asyncio
import csv
import os
from datetime import datetime
from typing import List, Dict

from dotenv import load_dotenv
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import MemcmpOpts, DataSliceOpts
from solders.pubkey import Pubkey
from solders.signature import Signature


TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
ASSOCIATED_TOKEN_PROGRAM_ID = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")

def get_associated_token_address(mint: str, owner: str) -> str:
    """Get the associated token address for a given mint and owner"""
    address, _ = Pubkey.find_program_address(seeds=[
        bytes(Pubkey.from_string(owner)),
        bytes(TOKEN_PROGRAM_ID),
        bytes(Pubkey.from_string(mint))
    ], program_id=ASSOCIATED_TOKEN_PROGRAM_ID)

    return str(address)

class TokenTransferExtractor:
    def __init__(self, token_mint:str, rpc_url: str = "https://api.mainnet-beta.solana.com"):
        """
        Initialize the Token transfer extractor
        
        Args:
            rpc_url: Solana RPC endpoint URL
        """
        self.client = AsyncClient(rpc_url)
        self.token_mint = token_mint
        
    async def get_token_accounts(self) -> List[str]:
        """Get all token accounts that hold  tokens"""
        try:
            # Get all token accounts for the  mint
            response = await self.client.get_program_accounts(
                pubkey=Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),  # SPL Token program
                data_slice=DataSliceOpts(0, 64),
                filters=[
                    165,
                    MemcmpOpts(
                    0,
                    self.token_mint
                    ),
                ]
            )
            # print("response", response)
            
            accounts = []
            for account in response.value:
                accounts.append(str(account.pubkey))
            
            return accounts
            
        except Exception as e:
            print(f"Error getting token accounts: {e.__cause__} {e}")
            return []
    
    async def get_account_transactions(self, account: str, limit: int = 1000) -> List[Dict]:
        """Get transaction signatures for a specific account"""
        try:
            response = await self.client.get_signatures_for_address(
                Pubkey.from_string(account),
                limit=limit
            )
            
            return [
                {
                    'signature': str(sig.signature),
                    'slot': sig.slot,
                    'block_time': sig.block_time,
                    'confirmation_status': sig.confirmation_status
                }
                for sig in response.value
            ]
            
        except Exception as e:
            print(f"Error getting signatures for {account}: {e.__cause__} {e}")
            return []
    
    async def parse_transaction(self, signature: str) -> List[Dict]:
        """Parse a transaction to extract  transfer details"""
        try:
            # Get transaction details
            response = await self.client.get_transaction(
                Signature.from_string(signature),
                max_supported_transaction_version=0
            )
            
            if not response.value:
                # print(f"Transaction: {signature} not found or invalid")
                return []
            
            tx = response.value
            
            # Parse the transaction for token transfers
            transfers= []
            
            # Check if transaction was successful
            if tx.transaction.meta and tx.transaction.meta.err:
                # print(f"Transaction {signature} failed with error: {str(tx.transaction.meta.err)}")
                return []
            
            # Parse token balances changes
            if tx.transaction.meta and tx.transaction.meta.pre_token_balances and tx.transaction.meta.post_token_balances:
                pre_balances = {bal.account_index: bal for bal in tx.transaction.meta.pre_token_balances}
                post_balances = {bal.account_index: bal for bal in tx.transaction.meta.post_token_balances}

                # Find balance changes for  token
                for account_index, post_bal in post_balances.items():

                    if post_bal.mint == Pubkey.from_string(self.token_mint):
                        pre_bal = pre_balances.get(account_index)
                        if pre_bal:
                            pre_amount = int(pre_bal.ui_token_amount.amount)
                            post_amount = int(post_bal.ui_token_amount.amount)
                            change = post_amount - pre_amount
                            
                            if change != 0: 
                                transfer = {
                                    'signature': signature,
                                    'slot': tx.slot,
                                    'block_time': datetime.fromtimestamp(tx.block_time) if tx.block_time else None,
                                    'account': get_associated_token_address(self.token_mint, str(post_bal.owner or Pubkey.default())),
                                    'owner': str(post_bal.owner or Pubkey.default()),
                                    'amount_change': change,
                                    'amount_change_ui': change / (10 ** post_bal.ui_token_amount.decimals),
                                    'pre_balance': pre_amount,
                                    'post_balance': post_amount,
                                    'decimals': post_bal.ui_token_amount.decimals
                                }
                                # print("Transfer found:", transfer)
                                transfers.append(transfer)
                                
            print(f"Transaction {signature}: Found {len(transfers)} transfers")
            
            return transfers
            
        except Exception as e:
            print(f"Error parsing transaction {signature}: {e.__cause__} {e}")
            return []
    
    async def extract_all_transfers(self, max_accounts: int = 100, max_tx_per_account: int = 1000) -> List[Dict]:
        """Extract all  token transfers"""
        print("Getting  token accounts...")
        token_accounts = await self.get_token_accounts()
        
        if len(token_accounts) == 0:
            print("No token accounts found. Please verify the  mint address.")
            return []
        
        print(f"Found {len(token_accounts)} token accounts")
        
        # Limit accounts to process (for performance)
        if len(token_accounts) > max_accounts:
            token_accounts = token_accounts[:max_accounts]
            print(f"Processing first {max_accounts} accounts")
        
        all_transfers = []
        processed_signatures = set()  # To avoid duplicates
        
        for i, account in enumerate(token_accounts):
            print(f"Processing account {i+1}/{len(token_accounts)}: {account}")
            
            # Get transaction signatures for this account
            signatures = await self.get_account_transactions(account, max_tx_per_account)
            
            if not signatures:
                continue
            
            print(f"  Found {len(signatures)} transactions")
            
            # Process each transaction
            for j, sig_info in enumerate(signatures):
                signature = sig_info['signature']
                
                # Skip if already processed
                if signature in processed_signatures:
                    continue
                
                processed_signatures.add(signature)
                
                # Parse transaction
                transfers = await self.parse_transaction(signature)
                
                if len(transfers) > 0:
                    all_transfers.extend(transfers)
                    print(f"  Transaction {j+1}: Found {len(transfers)} transfers")
                
                # Add small delay to avoid rate limiting
                await asyncio.sleep(0.1)
        
        return all_transfers
    
    async def save_to_csv(self, transfers: List[Dict], filename: str = "token_transfers.csv"):
        """Save transfers to CSV file"""
        if len(transfers) == 0:
            print("No transfers to save")
            return
        
        fieldnames = [
            'signature', 'slot', 'block_time', 'account', 'owner',
            'amount_change', 'amount_change_ui', 'pre_balance', 
            'post_balance', 'decimals'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for transfer in transfers:
                writer.writerow(transfer)
        
        print(f"Saved {len(transfers)} transfers to {filename}")
    
    async def close(self):
        """Close the RPC client"""
        await self.client.close()


async def main():
    """Main function to run the extraction"""
    load_dotenv()

    RPC_URL = os.getenv("RPC_URL")
    TOKEN_MINT = os.getenv("TOKEN_MINT")

    if not RPC_URL or not TOKEN_MINT:
        print("Please set RPC_URL and TOKEN_MINT in the .env file")
        return

    # Initialize the extractor
    extractor = TokenTransferExtractor(token_mint=TOKEN_MINT, rpc_url=RPC_URL)
    
    try:
        print("Starting  token transfer extraction...")
        
        # Extract all transfers
        transfers = await extractor.extract_all_transfers(
            max_accounts=1,  # Adjust based on your needs
            max_tx_per_account=1000  # Adjust based on your needs
        )
        
        # Save to CSV
        await extractor.save_to_csv(transfers)
        
        print(f"Extraction complete! Found {len(transfers)} total transfers")
        
    except Exception as e:
        print(f"Error during extraction: {e}")
    
    finally:
        await extractor.close()


if __name__ == "__main__":
    # Install required packages:
    # pip install solana solders
    
    print("Solana Token Tx Transfer Extractor")
    print("")
    
    # Run the extraction
    asyncio.run(main())