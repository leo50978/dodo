const admin = require("firebase-admin");

function parseArgs(argv) {
  const args = Array.from(argv || []);
  const out = {
    email: "",
    uid: "",
    remove: false,
  };

  for (let i = 0; i < args.length; i += 1) {
    const arg = String(args[i] || "").trim();
    if (arg === "--email") {
      out.email = String(args[i + 1] || "").trim();
      i += 1;
      continue;
    }
    if (arg === "--uid") {
      out.uid = String(args[i + 1] || "").trim();
      i += 1;
      continue;
    }
    if (arg === "--remove") {
      out.remove = true;
    }
  }

  return out;
}

function printUsage() {
  console.log("Usage:");
  console.log("  node set-admin-claim.js --email you@example.com");
  console.log("  node set-admin-claim.js --uid FIREBASE_UID");
  console.log("  node set-admin-claim.js --email you@example.com --remove");
  console.log("");
  console.log("Prerequisite:");
  console.log("  Provide Admin SDK credentials, for example via GOOGLE_APPLICATION_CREDENTIALS.");
}

async function findUser(options) {
  const auth = admin.auth();
  if (options.uid) {
    return auth.getUser(options.uid);
  }
  if (options.email) {
    return auth.getUserByEmail(options.email);
  }
  throw new Error("Email ou UID requis.");
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (!options.email && !options.uid) {
    printUsage();
    process.exitCode = 1;
    return;
  }

  admin.initializeApp();

  const user = await findUser(options);
  const currentClaims = (user.customClaims && typeof user.customClaims === "object")
    ? { ...user.customClaims }
    : {};

  if (options.remove) {
    delete currentClaims.admin;
  } else {
    currentClaims.admin = true;
  }

  await admin.auth().setCustomUserClaims(user.uid, currentClaims);

  const action = options.remove ? "retiré" : "attribué";
  console.log(`Claim admin ${action} pour: ${user.email || user.uid}`);
  console.log(`UID: ${user.uid}`);
  console.log("Reconnecte-toi (ou force un refresh du token) avant de rouvrir Dpayment.");
}

main().catch((err) => {
  console.error("Impossible de mettre à jour le claim admin.");
  console.error(err?.message || err);
  process.exitCode = 1;
});
