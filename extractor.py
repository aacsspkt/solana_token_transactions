import asyncio
import csv
import os
from datetime import datetime
from typing import List, Dict, Optional

from construct import max_
from dotenv import load_dotenv
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import MemcmpOpts, DataSliceOpts
from solders.pubkey import Pubkey
from solders.signature import Signature
import argparse


TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
ASSOCIATED_TOKEN_PROGRAM_ID = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")

def get_associated_token_address(mint: Pubkey, owner: Pubkey) -> Pubkey:
    """Get the associated token address for a given mint and owner"""
    address, _ = Pubkey.find_program_address(seeds=[
        bytes(owner),
        bytes(TOKEN_PROGRAM_ID),
        bytes(mint)
    ], program_id=ASSOCIATED_TOKEN_PROGRAM_ID)

    return address

class TokenTransferExtractor:
    def __init__(self, token_mint: Pubkey, rpc_url: str):
        """
        Initialize the Token transfer extractor
        
        Args:
            token_mint: The mint address of the token to track
            rpc_url: Solana RPC endpoint URL
        """
        self.client = AsyncClient(rpc_url)
        self.token_mint = token_mint
        
    async def get_token_accounts(self, user: Optional[Pubkey] = None) -> List[str]:
        """Get all token accounts that hold tokens"""
        try:
            filters = [
                165,  # Token account data size
                MemcmpOpts(
                    0,
                    str(self.token_mint)
                ),
            ]

            if user is not None:
                filters.append(MemcmpOpts(
                    32,
                    str(user)  # Filter by user address
                ))
                
            # Get all token accounts for the mint
            response = await self.client.get_program_accounts(
                pubkey=TOKEN_PROGRAM_ID,
                data_slice=DataSliceOpts(0, 0),
                filters=filters
            )
            
            accounts = []
            for account in response.value:
                accounts.append(str(account.pubkey))
            
            return accounts
            
        except Exception as e:
            print(f"Error getting token accounts: {e.__cause__}")
            print(e)
            return []
    
    async def get_account_transactions(self, account: str, limit: int = 1000) -> List[str]:
        """Get transaction signatures for a specific account"""
        try:
            response = await self.client.get_signatures_for_address(
                Pubkey.from_string(account),
                limit=limit
            )
            
            return [str(sig.signature) for sig in response.value]
            
        except Exception as e:
            print(f"Error getting signatures for {account}: {e.__cause__}")
            print(e)
            return []
    
    async def parse_transaction_for_transfer(self, signature: str, sender: Optional[Pubkey], receiver: Optional[Pubkey]) -> List[Dict]:
        """Parse a transaction to extract token transfer details between sender and receiver"""
        try:
            # Get transaction details
            response = await self.client.get_transaction(
                Signature.from_string(signature),
                max_supported_transaction_version=0
            )
            
            if not response.value:
                return []
            
            tx = response.value
            
            # Check if transaction was successful
            if tx.transaction.meta and tx.transaction.meta.err:
                return []
            
            transfers = []
            
            # Parse token balances changes
            if tx.transaction.meta and tx.transaction.meta.pre_token_balances and tx.transaction.meta.post_token_balances:
                pre_balances = {bal.account_index: bal for bal in tx.transaction.meta.pre_token_balances}
                post_balances = {bal.account_index: bal for bal in tx.transaction.meta.post_token_balances}

                # Collect all balance changes for this token
                balance_changes = {}
                
                for account_index, post_bal in post_balances.items():
                    if post_bal.mint == self.token_mint:
                        pre_bal = pre_balances.get(account_index)
                        if pre_bal:
                            pre_amount = int(pre_bal.ui_token_amount.amount)
                            post_amount = int(post_bal.ui_token_amount.amount)
                            change = post_amount - pre_amount
                            
                            if change != 0:
                                owner = post_bal.owner or Pubkey.default()
                                balance_changes[str(owner)] = {
                                    'owner': str(owner),
                                    'account': str(get_associated_token_address(mint=self.token_mint, owner=owner)),
                                    'amount_change': change,
                                    'amount_change_ui': change / (10 ** post_bal.ui_token_amount.decimals),
                                    'pre_balance': pre_amount,
                                    'post_balance': post_amount,
                                    'decimals': post_bal.ui_token_amount.decimals
                                }

                # Find matching sender-receiver pairs
                for sender_addr, sender_data in balance_changes.items():
                    if sender_data['amount_change'] < 0:  # Sender (negative change)
                        for receiver_addr, receiver_data in balance_changes.items():
                            if receiver_data['amount_change'] > 0:  # Receiver (positive change)
                                # Check if this matches our filter criteria
                                sender_match = sender is None or str(sender) == sender_addr
                                receiver_match = receiver is None or str(receiver) == receiver_addr
                                
                                if sender_match and receiver_match:
                                    transfer = {
                                        'signature': signature,
                                        'slot': tx.slot,
                                        'block_time': datetime.fromtimestamp(tx.block_time) if tx.block_time else None,
                                        'sender_address': sender_addr,
                                        'sender_account': sender_data['account'],
                                        'receiver_address': receiver_addr,
                                        'receiver_account': receiver_data['account'],
                                        'amount_transferred': abs(sender_data['amount_change']),
                                        'amount_transferred_ui': abs(sender_data['amount_change_ui']),
                                        'sender_pre_balance': sender_data['pre_balance'],
                                        'sender_post_balance': sender_data['post_balance'],
                                        'receiver_pre_balance': receiver_data['pre_balance'],
                                        'receiver_post_balance': receiver_data['post_balance'],
                                    }
                                    transfers.append(transfer)
                                    
            # if len(transfers) > 0:
            print(f"  Transaction {signature}: Found {len(transfers)} transfers")
            
            return transfers
            
        except Exception as e:
            print(f"Error parsing transaction {signature}: {e.__cause__}")
            print(e)
            return []
    
    async def extract_sender_receiver_transfers(self, sender: Optional[Pubkey], receiver: Optional[Pubkey], max_accounts: int = 100000, max_tx_per_account: int = 1000) -> List[Dict]:
        """Extract token transfers between sender and receiver"""
        
        # Get token accounts for both sender and receiver if specified
        accounts_to_process = set()
        
        if sender:
            print(f"Getting token accounts for sender: {sender}")
            sender_accounts = await self.get_token_accounts(user=sender)
            accounts_to_process.update(sender_accounts)
            print(f"Found {len(sender_accounts)} sender token accounts")
        
        if receiver:
            print(f"Getting token accounts for receiver: {receiver}")
            receiver_accounts = await self.get_token_accounts(user=receiver)
            accounts_to_process.update(receiver_accounts)
            print(f"Found {len(receiver_accounts)} receiver token accounts")
        
        # If neither sender nor receiver specified, get all token accounts
        if not sender and not receiver:
            print("Getting all token accounts...")
            all_accounts = await self.get_token_accounts()
            accounts_to_process.update(all_accounts)
            print(f"Found {len(all_accounts)} total token accounts")
        
        if len(accounts_to_process) == 0:
            print("No token accounts found. Please verify the token mint address and user addresses.")
            return []
        
        accounts_list = list(accounts_to_process)
        
        # Limit accounts to process (for performance)
        if len(accounts_list) > max_accounts:
            accounts_list = accounts_list[:max_accounts]
            print(f"Processing first {max_accounts} accounts")
        
        all_transfers = []
        processed_signatures = set()  # To avoid duplicates
        
        for i, account in enumerate(accounts_list):
            print(f"Processing account {i+1}/{len(accounts_list)}: {account}")
            
            # Get transaction signatures for this account
            signatures = await self.get_account_transactions(account, max_tx_per_account)
            
            if len(signatures) == 0:
                continue
            
            print(f"  Found {len(signatures)} transactions")
            
            # Process each transaction
            for j, signature in enumerate(signatures):                
                # Skip if already processed
                if signature in processed_signatures:
                    print(f"  Skipping already processed signature: {signature}")
                    continue
                
                processed_signatures.add(signature)
                
                # Parse transaction for sender-receiver transfers
                transfers = await self.parse_transaction_for_transfer(signature, sender, receiver)
                
                if len(transfers) > 0:
                    all_transfers.extend(transfers)
                
                # Add small delay to avoid rate limiting
                await asyncio.sleep(0.1)
        
        return all_transfers
    
    async def save_to_csv(self, transfers: List[Dict], filename: str):
        """Save transfers to CSV file"""
        if len(transfers) == 0:
            print("No transfers to save")
            return
        
        fieldnames = [
            'signature', 'slot', 'block_time', 
            'sender_address', 'sender_account',
            'receiver_address', 'receiver_account',
            'amount_transferred', 'amount_transferred_ui',
            'sender_pre_balance', 'sender_post_balance',
            'receiver_pre_balance', 'receiver_post_balance'
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

    if not RPC_URL:
        print("Please set RPC_URL in the .env file")
        return

    parser = argparse.ArgumentParser(description='Solana Token Transfer Extractor - Sender to Receiver')
    parser.add_argument('mint', type=str, help='Token mint address to extract transfers')
    parser.add_argument('-s', '--sender', type=str, help='Sender address to filter transfers', default=None)
    parser.add_argument('-r', '--receiver', type=str, help='Receiver address to filter transfers', default=None)
    parser.add_argument('-f', '--filename', type=str, help='Filename to save transfer data. No need to include file extension.', default="token_transfers")
    parser.add_argument('-a', '--max_accounts', type=int, help='Maximum number of accounts to process', default=100000)
    parser.add_argument('-t', '--max_tx_per_account', type=int, help='Maximum transactions per account to process', default=1000)
    args = parser.parse_args()

    token_mint = args.mint
    sender = args.sender
    receiver = args.receiver
    filename = str(args.filename)
    max_accounts = int(args.max_accounts)
    max_tx_per_account = int(args.max_tx_per_account)

    if not token_mint:
        print("Please provide a token mint address")
        return

    if max_accounts < 1:
        print("max_accounts must be at least 1")
        return

    if max_tx_per_account < 1:
        print("max_tx_per_account must be at least 1")
        return

    filename = f"{filename}.csv" 
    token_mint = Pubkey.from_string(token_mint)

    sender_pubkey = None
    receiver_pubkey = None
    
    if sender:
        sender_pubkey = Pubkey.from_string(sender)
        print(f"Filtering by sender: {sender}")
    
    if receiver:
        receiver_pubkey = Pubkey.from_string(receiver)
        print(f"Filtering by receiver: {receiver}")
    
    if not sender and not receiver:
        print("No sender or receiver specified - will extract all transfers")
    
    # Initialize the extractor
    extractor = TokenTransferExtractor(token_mint=token_mint, rpc_url=RPC_URL)
    
    try:
        print("Starting token transfer extraction...")
        
        # Extract sender-receiver transfers
        transfers = await extractor.extract_sender_receiver_transfers(
            sender=sender_pubkey,
            receiver=receiver_pubkey,
            max_accounts=max_accounts,
            max_tx_per_account=max_tx_per_account
        )
        
        # Save to CSV
        await extractor.save_to_csv(transfers, filename)
        
        print(f"Extraction complete! Found {len(transfers)} total transfers")
        
    except Exception as e:
        print(f"Error during extraction: {e}")
    
    finally:
        await extractor.close()


if __name__ == "__main__":    
    print("Solana Token Transfer Extractor - Sender to Receiver")
    print("")
    
    # Run the extraction
    asyncio.run(main())