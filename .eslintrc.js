module.exports = {
    "parser": "babel-eslint",
    "env": {
        "es6": true,
        "node": true,
    },
    "ecmaFeatures": {
        "classes": true,
    },
    "extends": "eslint:recommended",
    "rules": {
        "indent": [
            "error",
            4,
        ],
        "linebreak-style": [
            "error",
            "unix",
        ],
        "no-console": 0,
        "quotes": [
            "error",
            "single",
            {
                "allowTemplateLiterals": true,
            },
        ],
        "semi": [
            "error",
            "always",
        ],
    },
};
