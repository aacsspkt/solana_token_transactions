"""
Solana Token Transfer Extractor - Enhanced Version

This module extracts token transfers from the Solana blockchain for a specific token mint,
with optional filtering by sender and/or receiver addresses.
"""

import asyncio
import csv
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple
from pathlib import Path

from dotenv import load_dotenv
from solana.rpc.async_api import AsyncClient
from solana.rpc.core import RPCException
from solana.rpc.types import MemcmpOpts, DataSliceOpts
from solders.pubkey import Pubkey
from solders.signature import Signature
import argparse


# Constants
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
ASSOCIATED_TOKEN_PROGRAM_ID = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
TOKEN_ACCOUNT_DATA_SIZE = 165
MAX_SIGNATURES_PER_REQUEST = 1000
DEFAULT_RATE_LIMIT_DELAY = 0.01  # seconds


@dataclass
class TokenTransfer:
    """Data class representing a token transfer"""
    signature: str
    slot: int
    block_time: Optional[datetime]
    sender_address: str
    sender_account: str
    receiver_address: str
    receiver_account: str
    amount_transferred: int
    amount_transferred_ui: float
    sender_pre_balance: int
    sender_post_balance: int
    receiver_pre_balance: int
    receiver_post_balance: int


@dataclass
class BalanceChange:
    """Data class representing a balance change in a transaction"""
    owner: str
    account: str
    amount_change: int
    amount_change_ui: float
    pre_balance: int
    post_balance: int
    decimals: int


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def get_associated_token_address(mint: Pubkey, owner: Pubkey) -> Pubkey:
    """Get the associated token address for a given mint and owner"""
    try:
        address, _ = Pubkey.find_program_address(
            seeds=[bytes(owner), bytes(TOKEN_PROGRAM_ID), bytes(mint)], 
            program_id=ASSOCIATED_TOKEN_PROGRAM_ID
        )
        return address
    except Exception as e:
        raise ValueError(f"Failed to derive associated token address: {e}")


def validate_pubkey(address: str, name: str) -> Pubkey:
    """Validate and convert string address to Pubkey"""
    try:
        return Pubkey.from_string(address)
    except Exception:
        raise ValueError(f"Invalid {name} address: {address}")


class TokenTransferExtractor:
    """
    Extracts token transfers from the Solana blockchain for a specific token mint.
    
    This class provides functionality to:
    - Find token accounts for a specific mint
    - Extract transaction signatures for accounts
    - Parse transactions to identify token transfers
    - Filter transfers by sender/receiver
    - Export results to CSV
    """
    
    def __init__(self, token_mint: Pubkey, rpc_url: str, logger: Optional[logging.Logger] = None):
        """
        Initialize the Token transfer extractor
        
        Args:
            token_mint: The mint address of the token to track
            rpc_url: Solana RPC endpoint URL
            logger: Optional logger instance
        """
        self.client = AsyncClient(rpc_url)
        self.token_mint = token_mint
        self.logger = logger or setup_logging()
        
    async def get_token_accounts(self, user: Optional[Pubkey] = None) -> List[str]:
        """
        Get all token accounts that hold the specified token
        
        Args:
            user: Optional user address to filter accounts by owner
            
        Returns:
            List of token account addresses as strings
        """
        try:
            filters = [
                TOKEN_ACCOUNT_DATA_SIZE,  # Token account data size
                MemcmpOpts(offset=0, bytes=str(self.token_mint)),  # Filter by mint
            ]

            if user is not None:
                filters.append(MemcmpOpts(offset=32, bytes=str(user)))  # Filter by owner
                
            response = await self.client.get_program_accounts(
                pubkey=TOKEN_PROGRAM_ID,
                data_slice=DataSliceOpts(0, 0),
                filters=filters
            )
            
            return [str(account.pubkey) for account in response.value]
            
        except RPCException as e:
            self.logger.error(f"RPC error getting token accounts: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error getting token accounts: {e}")
            return []
    
    async def get_all_account_transactions(self, account: str) -> List[Dict]:
        """
        Get all transaction signatures for a specific account using pagination
        
        Args:
            account: The account address to get transactions for
            
        Returns:
            List of transaction information dictionaries
        """
        all_signatures: List[Dict] = []
        before_signature = None
        
        try:
            account_pubkey = Pubkey.from_string(account)
            
            while True:
                response = await self.client.get_signatures_for_address(
                    account_pubkey,
                    before=before_signature,
                    limit=MAX_SIGNATURES_PER_REQUEST
                )
                
                batch_signatures = [
                    {
                        "signature": str(sig.signature),
                        "slot": sig.slot,
                        "block_time": sig.block_time
                    } for sig in response.value
                ]
                
                if not batch_signatures:
                    break
                
                all_signatures.extend(batch_signatures)
                self.logger.info(
                    f"Fetched {len(batch_signatures)} signatures for {account}. "
                    f"Total: {len(all_signatures)}"
                )
                
                if len(batch_signatures) < MAX_SIGNATURES_PER_REQUEST:
                    break
                    
                before_signature = response.value[-1].signature
                
        except Exception as e:
            self.logger.error(f"Error getting signatures for {account}: {e}")
            
        return all_signatures
    
    def _parse_balance_changes(self, tx_meta) -> Dict[str, BalanceChange]:
        """
        Parse token balance changes from transaction metadata
        
        Args:
            tx_meta: Transaction metadata containing balance information
            
        Returns:
            Dictionary mapping owner addresses to their balance changes
        """
        if not (tx_meta and tx_meta.pre_token_balances and tx_meta.post_token_balances):
            return {}
            
        pre_balances = {bal.account_index: bal for bal in tx_meta.pre_token_balances}
        post_balances = {bal.account_index: bal for bal in tx_meta.post_token_balances}
        balance_changes = {}
        
        for account_index, post_bal in post_balances.items():
            if post_bal.mint != self.token_mint:
                continue
                
            pre_bal = pre_balances.get(account_index)
            if not pre_bal:
                continue
                
            pre_amount = int(pre_bal.ui_token_amount.amount)
            post_amount = int(post_bal.ui_token_amount.amount)
            change = post_amount - pre_amount
            
            if change == 0:
                continue
                
            owner = post_bal.owner or Pubkey.default()
            decimals = post_bal.ui_token_amount.decimals
            
            balance_changes[str(owner)] = BalanceChange(
                owner=str(owner),
                account=str(get_associated_token_address(self.token_mint, owner)),
                amount_change=change,
                amount_change_ui=change / (10 ** decimals),
                pre_balance=pre_amount,
                post_balance=post_amount,
                decimals=decimals
            )
            
        return balance_changes
    
    def _find_transfer_pairs(
        self, 
        balance_changes: Dict[str, BalanceChange],
        sender_filter: Optional[Pubkey],
        receiver_filter: Optional[Pubkey]
    ) -> List[Tuple[BalanceChange, BalanceChange]]:
        """
        Find matching sender-receiver pairs from balance changes
        
        Args:
            balance_changes: Dictionary of balance changes by owner
            sender_filter: Optional sender address filter
            receiver_filter: Optional receiver address filter
            
        Returns:
            List of (sender, receiver) balance change pairs
        """
        pairs = []
        
        for sender_addr, sender_data in balance_changes.items():
            if sender_data.amount_change >= 0:  # Skip non-senders
                continue
                
            for receiver_addr, receiver_data in balance_changes.items():
                if receiver_data.amount_change <= 0:  # Skip non-receivers
                    continue
                    
                # Check filter criteria
                sender_match = sender_filter is None or str(sender_filter) == sender_addr
                receiver_match = receiver_filter is None or str(receiver_filter) == receiver_addr
                
                if sender_match and receiver_match:
                    pairs.append((sender_data, receiver_data))
                    
        return pairs
    
    async def parse_transaction_for_transfer(
        self,
        signature: str, 
        sender_filter: Optional[Pubkey] = None, 
        receiver_filter: Optional[Pubkey] = None
    ) -> List[TokenTransfer]:
        """
        Parse a transaction to extract token transfer details
        
        Args:
            signature: Transaction signature to parse
            sender_filter: Optional sender address filter
            receiver_filter: Optional receiver address filter
            
        Returns:
            List of TokenTransfer objects found in the transaction
        """
        try:
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
            
            # Parse balance changes
            balance_changes = self._parse_balance_changes(tx.transaction.meta)
            if not balance_changes:
                return []
                
            # Find transfer pairs
            transfer_pairs = self._find_transfer_pairs(
                balance_changes, sender_filter, receiver_filter
            )
            
            transfers = []
            for sender_data, receiver_data in transfer_pairs:
                transfer = TokenTransfer(
                    signature=signature,
                    slot=tx.slot,
                    block_time=datetime.fromtimestamp(tx.block_time) if tx.block_time else None,
                    sender_address=sender_data.owner,
                    sender_account=sender_data.account,
                    receiver_address=receiver_data.owner,
                    receiver_account=receiver_data.account,
                    amount_transferred=abs(sender_data.amount_change),
                    amount_transferred_ui=abs(sender_data.amount_change_ui),
                    sender_pre_balance=sender_data.pre_balance,
                    sender_post_balance=sender_data.post_balance,
                    receiver_pre_balance=receiver_data.pre_balance,
                    receiver_post_balance=receiver_data.post_balance,
                )
                transfers.append(transfer)
            
            if transfers:
                self.logger.info(f"Transaction {signature}: Found {len(transfers)} transfers")
                
            return transfers
            
        except Exception as e:
            self.logger.error(f"Error parsing transaction {signature}: {e}")
            return []
    
    async def _get_common_transactions(
        self, 
        sender_accounts: List[str], 
        receiver_accounts: List[str]
    ) -> Set[str]:
        """
        Get transactions common between sender and receiver accounts
        
        Args:
            sender_accounts: List of sender token accounts
            receiver_accounts: List of receiver token accounts
            
        Returns:
            Set of common transaction signatures
        """
        sender_sigs = set()
        receiver_sigs = set()
        
        # Get all sender signatures
        for account in sender_accounts:
            sigs = await self.get_all_account_transactions(account)
            sender_sigs.update(sig['signature'] for sig in sigs)
            
        # Get all receiver signatures
        for account in receiver_accounts:
            sigs = await self.get_all_account_transactions(account)
            receiver_sigs.update(sig['signature'] for sig in sigs)
            
        return sender_sigs.intersection(receiver_sigs)
    
    async def extract_transfers(
        self, 
        sender: Optional[Pubkey] = None, 
        receiver: Optional[Pubkey] = None, 
        max_accounts: int = 100000,
    ) -> List[TokenTransfer]:
        """
        Extract token transfers with optional sender/receiver filtering
        
        Args:
            sender: Optional sender address filter
            receiver: Optional receiver address filter
            max_accounts: Maximum number of accounts to process
            
        Returns:
            List of TokenTransfer objects
        """
        accounts_to_process: Set[str] = set()

        # Collect relevant token accounts
        if sender:
            self.logger.info(f"Getting token accounts for sender: {sender}")
            sender_accounts = await self.get_token_accounts(user=sender)
            accounts_to_process.update(sender_accounts)
            self.logger.info(f"Found {len(sender_accounts)} sender token accounts")

        if receiver:
            self.logger.info(f"Getting token accounts for receiver: {receiver}")
            receiver_accounts = await self.get_token_accounts(user=receiver)
            accounts_to_process.update(receiver_accounts)
            self.logger.info(f"Found {len(receiver_accounts)} receiver token accounts")
            
        if not sender and not receiver:
            self.logger.info("Getting all token accounts...")
            all_accounts = await self.get_token_accounts()
            accounts_to_process.update(all_accounts)
            self.logger.info(f"Found {len(all_accounts)} total token accounts")
        
        if not accounts_to_process:
            self.logger.warning("No token accounts found")
            return []
        
        accounts_list = list(accounts_to_process)[:max_accounts]
        if len(accounts_to_process) > max_accounts:
            self.logger.info(f"Limited processing to first {max_accounts} accounts")
        
        all_transfers = []
        processed_signatures: Set[str] = set()

        # Optimize for specific sender-receiver case
        if sender and receiver and sender != receiver:
            sender_accounts = await self.get_token_accounts(user=sender)
            receiver_accounts = await self.get_token_accounts(user=receiver)
            
            common_sigs = await self._get_common_transactions(sender_accounts, receiver_accounts)
            self.logger.info(f"Found {len(common_sigs)} common transactions")
            
            for i, signature in enumerate(common_sigs, 1):
                if signature in processed_signatures:
                    continue
                    
                processed_signatures.add(signature)
                transfers = await self.parse_transaction_for_transfer(signature, sender, receiver)
                all_transfers.extend(transfers)
                
                if i % 100 == 0:
                    self.logger.info(f"Processed {i}/{len(common_sigs)} common transactions")
        else:
            # General case: process all accounts
            for j, account in enumerate(accounts_list, 1):
                self.logger.info(f"Processing account {j}/{len(accounts_list)}: {account}")
                
                signatures = await self.get_all_account_transactions(account)
                if not signatures:
                    continue
                
                self.logger.info(f"Found {len(signatures)} transactions for account")
                
                for k, sig_info in enumerate(signatures, 1):
                    signature = sig_info['signature']
                    
                    if signature in processed_signatures:
                        continue
                    
                    processed_signatures.add(signature)
                    transfers = await self.parse_transaction_for_transfer(signature, sender, receiver)
                    all_transfers.extend(transfers)
                    
                    if k % 100 == 0:
                        self.logger.info(f"Processed {k}/{len(signatures)} transactions for account")
        
        return all_transfers
    
    async def save_to_csv(self, transfers: List[TokenTransfer], filename: str):
        """
        Save transfers to CSV file
        
        Args:
            transfers: List of TokenTransfer objects to save
            filename: Output filename
        """
        if not transfers:
            self.logger.warning("No transfers to save")
            return
        
        # Ensure directory exists
        filepath = Path(filename)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = list(asdict(transfers[0]).keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for transfer in transfers:
                writer.writerow(asdict(transfer))
        
        self.logger.info(f"Saved {len(transfers)} transfers to {filename}")
    
    async def close(self):
        """Close the RPC client connection"""
        await self.client.close()


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Solana Token Transfer Extractor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract all transfers for a token
  python extractor.py So11111111111111111111111111111111111111112
  
  # Extract transfers from specific sender
  python extractor.py So11111111111111111111111111111111111111112 -s 9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM
  
  # Extract transfers between specific sender and receiver
  python extractor.py So11111111111111111111111111111111111111112 \\
    -s 9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM \\
    -r 3UVYmECPPMZSCqWKfENfuoTv51fTDTWicX9xmBD2euKe
        """
    )
    
    parser.add_argument('mint', help='Token mint address to extract transfers for')
    parser.add_argument('-s', '--sender', help='Sender address to filter transfers')
    parser.add_argument('-r', '--receiver', help='Receiver address to filter transfers')
    parser.add_argument('-f', '--filename', default='token_transfers.csv',
                       help='Output filename (default: token_transfers.csv)')
    parser.add_argument('-a', '--max-accounts', type=int, default=100000,
                       help='Maximum number of accounts to process (default: 100000)')
    parser.add_argument('-l', '--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level (default: INFO)')
    
    return parser.parse_args()


async def main():
    """Main function to run the extraction"""
    load_dotenv()
    
    args = parse_arguments()
    logger = setup_logging(args.log_level)
    
    # Validate environment
    rpc_url = os.getenv("RPC_URL")
    if not rpc_url:
        logger.error("Please set RPC_URL in the .env file")
        return 1

    try:
        # Validate and parse arguments
        token_mint = validate_pubkey(args.mint, "token mint")
        sender_pubkey = validate_pubkey(args.sender, "sender") if args.sender else None
        receiver_pubkey = validate_pubkey(args.receiver, "receiver") if args.receiver else None
        
        # Log configuration
        logger.info("=== Solana Token Transfer Extractor ===")
        logger.info(f"Token Mint: {token_mint}")
        if sender_pubkey:
            logger.info(f"Sender Filter: {sender_pubkey}")
        if receiver_pubkey:
            logger.info(f"Receiver Filter: {receiver_pubkey}")

        if sender_pubkey and receiver_pubkey and sender_pubkey == receiver_pubkey:
            logger.error(f"Sender filter and receiver filter cannot be same")
            return 1
            
        logger.info(f"Max Accounts: {args.max_accounts}")
        logger.info(f"Output File: {args.filename}")
        
        # Initialize extractor
        extractor = TokenTransferExtractor(token_mint=token_mint, rpc_url=rpc_url, logger=logger)
        
        try:
            logger.info("Starting token transfer extraction...")
            
            # Extract transfers
            transfers = await extractor.extract_transfers(
                sender=sender_pubkey,
                receiver=receiver_pubkey,
                max_accounts=args.max_accounts
            )
            
            # Save results
            await extractor.save_to_csv(transfers, args.filename)
            
            logger.info(f"Extraction complete! Found {len(transfers)} total transfers")
            return 0
            
        finally:
            await extractor.close()
            
    except ValueError as e:
        logger.error(f"Invalid input: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    print("Solana Token Transfer Extractor - Enhanced Version")
    print("=" * 50)
    exit_code = asyncio.run(main())
    exit(exit_code)