"""Generate a self-signed SSL certificate for AI Radio HTTPS.

Run once:  python generate_cert.py
Creates:   certs/radio.key  and  certs/radio.crt
"""
import subprocess
import sys
from pathlib import Path


def generate():
    cert_dir = Path("./certs")
    cert_dir.mkdir(exist_ok=True)

    key_path = cert_dir / "radio.key"
    cert_path = cert_dir / "radio.crt"

    if key_path.exists() and cert_path.exists():
        print(f"Certificate already exists: {cert_path}")
        print("Delete certs/ folder and re-run to regenerate.")
        return

    # Try using Python's cryptography library first
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        import datetime

        print("Generating self-signed certificate using cryptography library...")

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "AI Radio"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "AI Radio Station"),
        ])

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.utcnow())
            .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365 * 3))
            .add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName("localhost"),
                    x509.DNSName("*.local"),
                    x509.IPAddress(__import__("ipaddress").IPv4Address("127.0.0.1")),
                    x509.IPAddress(__import__("ipaddress").IPv4Address("0.0.0.0")),
                ]),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )

        key_path.write_bytes(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))

        print(f"✅ Certificate generated:")
        print(f"   Key:  {key_path}")
        print(f"   Cert: {cert_path}")
        print(f"   Valid for 3 years")
        print(f"\n⚠️  This is a self-signed certificate.")
        print(f"   Browsers will show a security warning — click 'Advanced' → 'Proceed' to accept.")
        return

    except ImportError:
        pass

    # Fallback: try openssl command
    try:
        cmd = [
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", str(key_path), "-out", str(cert_path),
            "-days", "1095", "-nodes",
            "-subj", "/CN=AI Radio/O=AI Radio Station",
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        print(f"✅ Certificate generated via openssl")
        return
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass

    print("❌ Could not generate certificate.")
    print("Install: pip install cryptography")
    sys.exit(1)


if __name__ == "__main__":
    generate()
