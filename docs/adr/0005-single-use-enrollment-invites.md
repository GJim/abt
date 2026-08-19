# Use single-use enrollment invites for worker and Trader admission

Worker and Trader enrollment uses a management-issued, role-bound, single-use invite valid for 60 minutes, rather than client-IP-based admission throttling. The controller cannot reliably obtain a real client IP through the available Cloudflare plan; an invite prevents unauthenticated registrations from consuming the bounded admission path, while the ledger stores only the invite hash and immutable issuance, use, expiry, or revocation events.
