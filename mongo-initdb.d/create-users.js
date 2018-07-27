util_aligner = connect("mongodb://localhost/util_aligner")

util_aligner.createUser({
    user: "util-aligner-server",
    pwd: "util-aligner-server",
    roles: ["readWrite"],
    authenticationRestrictions: [{
        clientSource: ["172.20.0.2"]
    }]
});

util_aligner.createUser({
    user: "util-aligner-api",
    pwd: "util-aligner-api",
    roles: ["read"]
});


admin = connect("mongodb://localhost/admin")

admin.createUser({
    user: "root",
    pwd: "root",
    roles: ["root"],
    authenticationRestrictions: [{
        clientSource: ["127.0.0.1"]
    }]
});
