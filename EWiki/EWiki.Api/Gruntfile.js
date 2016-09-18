'use strict';
module.exports = function (grunt) {

    // Load grunt tasks automatically
    require('load-grunt-tasks')(grunt);

    // Show grunt task time
    require('time-grunt')(grunt);

    // Configurable paths for the app
    var appConfig = {
        app: 'wwwroot/app',
        dist: 'wwwroot/dist'
    };

    // Grunt configuration
    grunt.initConfig({

        // Project settings
        ewikibd: appConfig,

        // The grunt server settings
        connect: {
            options: {
                port: 9000,
                hostname: 'localhost',
                livereload: 35729
            },
            livereload: {
                options: {
                    open: true,
                    middleware: function (connect) {
                        return [
                            connect.static('wwwroot/.tmp'),
                            connect().use(
                                'wwwroot/app/bower_components',
                                connect.static('wwwroot/app/bower_components')
                            ),
                            connect.static(appConfig.app)
                        ];
                    }
                }
            },
            dist: {
                options: {
                    open: true,
                    base: '<%= ewikibd.dist %>'
                }
            }
        },
        // Compile less to css
        less: {
            development: {
                options: {
                    compress: true,
                    optimization: 2
                },
                files: {
                    "wwwroot/app/styles/style.css": "wwwroot/app/less/style.less"
                }
            }
        },
        // Watch for changes in live edit
        watch: {
            styles: {
                files: ['wwwroot/app/less/**/*.less'],
                tasks: ['less', 'copy:styles'],
                options: {
                    nospawn: true,
                    livereload: '<%= connect.options.livereload %>'
                },
            },
            js: {
                files: ['<%= ewikibd.app %>/scripts/{,*/}*.js'],
                options: {
                    livereload: '<%= connect.options.livereload %>'
                }
            },
            livereload: {
                options: {
                    livereload: '<%= connect.options.livereload %>'
                },
                files: [
                    '<%= ewikibd.app %>/**/*.html',
                    'wwwroot/.tmp/styles/{,*/}*.css',
                    '<%= ewikibd.app %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}'
                ]
            }
        },
        // If you want to turn on uglify you will need write your angular code with string-injection based syntax
        // For example this is normal syntax: function exampleCtrl ($scope, $rootScope, $location, $http){}
        // And string-injection based syntax is: ['$scope', '$rootScope', '$location', '$http', function exampleCtrl ($scope, $rootScope, $location, $http){}]
        uglify: {
            options: {
                mangle: false
            }
        },
        // Clean dist folder
        clean: {
            dist: {
                files: [{
                    dot: true,
                    src: [
                        'wwwroot/.tmp',
                        '<%= ewikibd.dist %>/{,*/}*',
                        '!<%= ewikibd.dist %>/.git*'
                    ]
                }]
            },
            server: 'wwwroot/.tmp'
        },
        // Copies remaining files to places other tasks can use
        copy: {
            dist: {
                files: [
                    {
                        expand: true,
                        dot: true,
                        cwd: '<%= ewikibd.app %>',
                        dest: '<%= ewikibd.dist %>',
                        src: [
                            '*.{ico,png,txt}',
                            '.htaccess',
                            '*.html',
                            'views/{,*/}*.html',
                            'styles/patterns/*.*',
                            'img/{,*/}*.*'
                        ]
                    },
                    {
                        expand: true,
                        dot: true,
                        cwd: 'wwwroot/app/bower_components/fontawesome',
                        src: ['fonts/*.*'],
                        dest: '<%= ewikibd.dist %>'
                    },
                    {
                        expand: true,
                        dot: true,
                        cwd: 'wwwroot/app/bower_components/bootstrap',
                        src: ['fonts/*.*'],
                        dest: '<%= ewikibd.dist %>'
                    },
                ]
            },
            styles: {
                expand: true,
                cwd: '<%= ewikibd.app %>/styles',
                dest: 'wwwroot/.tmp/styles/',
                src: '{,*/}*.css'
            }
        },
        // Renames files for browser caching purposes
        filerev: {
            dist: {
                src: [
                    '<%= ewikibd.dist %>/scripts/{,*/}*.js',
                    '<%= ewikibd.dist %>/styles/{,*/}*.css',
                    '<%= ewikibd.dist %>/styles/fonts/*'
                ]
            }
        },
        htmlmin: {
            dist: {
                options: {
                    collapseWhitespace: true,
                    conservativeCollapse: true,
                    collapseBooleanAttributes: true,
                    removeCommentsFromCDATA: true,
                    removeOptionalTags: true
                },
                files: [{
                    expand: true,
                    cwd: '<%= ewikibd.dist %>',
                    src: ['*.html', 'views/{,*/}*.html'],
                    dest: '<%= ewikibd.dist %>'
                }]
            }
        },
        useminPrepare: {
            html: 'wwwroot/app/index.html',
            options: {
                dest: 'wwwroot/dist'
            }
        },
        usemin: {
            html: ['wwwroot/dist/index.html']
        }
    });

    // Run live version of app
    grunt.registerTask('live', [
        'clean:server',
        'copy:styles',
        'connect:livereload',
        'watch'
    ]);

    // Run build version of app
    grunt.registerTask('server', [
        'build',
        'connect:dist:keepalive'
    ]);

    // Build version for production
    grunt.registerTask('build', [
        'clean:dist',
        'less',
        'useminPrepare',
        'concat',
        'copy:dist',
        'cssmin',
        'uglify',
        'filerev',
        'usemin',
        'htmlmin'
    ]);

};
